from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime
from loguru import logger
import pandas as pd

from src.core.base_processor import BaseProcessor, ProcessingResult

try:
    from src.utils.config import settings
except ImportError:
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent.parent))
    from src.utils.config import settings

from src.connect.trino_client import DataLakeManager


class LandingToRawProcessor(BaseProcessor):
    """Process landing data to raw layer with ingestion_date partitioning"""
    
    def __init__(self, processor_id: str = "landing_to_raw_processor"):
        # Don't call super().__init__ to avoid DuckDB initialization
        self.processor_id = processor_id
        self.description = "Copy landing data to raw layer with ingestion_date partitioning"
        
        # Initialize Trino Data Lake Manager
        try:
            self.datalake = DataLakeManager()
            logger.info("Trino Data Lake Manager initialized")
            self.use_trino = True
        except Exception as e:
            logger.error(f"Failed to initialize Trino: {e}")
            # Fallback to DuckDB for now
            super().__init__(processor_id, self.description)
            self.use_trino = False
            return
            
        self.landing_prefix = settings.LANDING_PREFIX  # MinIO landing bucket path
        self.raw_prefix = settings.RAW_PREFIX
        self.ingestion_date = datetime.now().strftime("%Y-%m-%d")  # Default to current date
        self._start_time = None
        self._end_time = None
        self.args = None
        
    def set_args(self, args):
        """Set arguments from job manager"""
        self.args = args
        # Override ingestion_date if provided via command line
        if args and hasattr(args, 'ingestion_date') and args.ingestion_date:
            self.ingestion_date = args.ingestion_date
            logger.info(f"Using specified ingestion_date: {self.ingestion_date}")
        else:
            logger.info(f"Using current date as ingestion_date: {self.ingestion_date}")
        
    def _extract(self) -> Dict[str, Any]:
        """Extract: List files from MinIO landing bucket"""
        logger.info("Extracting files from MinIO landing bucket")
        
        if not self.use_trino:
            return super()._extract()
        
        extracted_files = {}
        
        # List files in MinIO landing bucket
        try:
            files_in_landing = self.datalake.minio.list_objects(prefix=self.landing_prefix)
            
            for object_key in files_in_landing:
                file_name = object_key.split('/')[-1]  # Get filename from full path
                if file_name.endswith(('.csv', '.json', '.jsonl')):
                    # Extract table type and date from filename
                    stem = Path(file_name).stem
                    if '_' in stem and stem.split('_')[-1].count('-') == 2:
                        table_type = '_'.join(stem.split('_')[:-1])
                        file_date = stem.split('_')[-1]
                        
                        # Only process files matching the target ingestion_date
                        if file_date != self.ingestion_date:
                            logger.debug(f"     Skipping {file_name} (date {file_date} != target {self.ingestion_date})")
                            continue
                            
                    else:
                        # File without date suffix - use current ingestion_date
                        table_type = stem
                        file_date = self.ingestion_date
                    
                    file_info = {
                        'landing_key': object_key,
                        'name': file_name,
                        'table_type': table_type,
                        'file_date': file_date,
                        'size': 0,
                        'raw_key': f"{self.raw_prefix}/ingestion_date={file_date}/{file_name}"
                    }
                    extracted_files[table_type] = file_info
                    logger.debug(f"Found file: {file_name} -> {table_type} ({file_date})")
            
        except Exception as e:
            logger.error(f"Failed to list landing files: {e}")
            return {}
        
        logger.info(f"📊 Found {len(extracted_files)} files in landing bucket")
        return extracted_files
    
    def _transform(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform: No transformation - just pass through with partition paths"""
        logger.info("No transformation - preparing for direct copy with ingestion_date partition")
        
        if not self.use_trino:
            return super()._transform(extracted_data)

        return extracted_data
    
    def _load(self, transformed_data: Dict[str, Any]) -> ProcessingResult:
        """Load: Copy files from landing to raw bucket with ingestion_date partitioning"""
        logger.info("Copying files from landing to raw with ingestion_date partitioning")
        
        if not self.use_trino:
            return super()._load(transformed_data)
        
        successful_copies = 0
        failed_copies = []
        
        for table_type, file_info in transformed_data.items():
            try:
                # Simple copy operation: landing -> raw with partition
                success = self.datalake.minio.copy_object(
                    source_key=file_info['landing_key'],
                    target_key=file_info['raw_key']
                )
                
                if success:
                    logger.info(f"📁 Copied {file_info['name']} -> {file_info['raw_key']}")
                    successful_copies += 1
                else:
                    failed_copies.append({
                        'file': file_info['name'],
                        'error': 'Copy operation failed'
                    })
                    logger.error(f"Failed to copy {file_info['name']}")
                
            except Exception as e:
                failed_copies.append({
                    'file': file_info['name'],
                    'error': str(e)
                })
                logger.error(f"Failed to copy {file_info['name']}: {e}")
        
        success = len(failed_copies) == 0
        message = f"Copied {successful_copies} files to raw layer with ingestion_date partitioning"
        if failed_copies:
            message += f", {len(failed_copies)} failed"
        
        return ProcessingResult(
            success=success,
            message=message,
            metadata={
                'successful_copies': successful_copies,
                'failed_copies': failed_copies,
                'files_processed': list(transformed_data.keys()),
                'raw_prefix': self.raw_prefix,
                'ingestion_date': self.ingestion_date,
                'partitioned': True
            },
            rows_processed=len(transformed_data),
            tables_created=[]
        )
    
    def _post_process(self, load_result: ProcessingResult) -> None:
        """Post-process: Log raw layer copy summary"""
        logger.info("🔍 Raw layer copy summary")
        
        logger.info(f"Landing → Raw Copy Complete:")
        logger.info(f"   Files copied: {load_result.metadata['successful_copies']}")
        logger.info(f"   Partition: ingestion_date={load_result.metadata['ingestion_date']}")
        logger.info(f"      Raw path: {self.raw_prefix}/ingestion_date={self.ingestion_date}/")
        
        if load_result.metadata['failed_copies']:
            logger.warning(f"       Failed copies: {len(load_result.metadata['failed_copies'])}")
        
        logger.info("   Files are now available in raw layer for trusted transformation")
    
    def cleanup(self):
        """Cleanup resources"""
        if hasattr(self, 'datalake'):
            self.datalake.close()
        elif hasattr(self, 'lakehouse'):
            self.lakehouse.close()