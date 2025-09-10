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
from src.utils.schema_registry import (
    get_all_trusted_tables, build_view_ddl
)


class RawToTrustedProcessor(BaseProcessor):
    """Process raw data to trusted layer with parquet format conversion"""
    
    def __init__(self, processor_id: str = "raw_to_trusted_processor"):
        self.processor_id = processor_id
        self.description = "Transform raw data to trusted layer with parquet format"
        
        # Initialize Trino Data Lake Manager - mandatory
        self.datalake = DataLakeManager()
        logger.info("Trino Data Lake Manager initialized")
            
        self.raw_prefix = settings.RAW_PREFIX
        self.trusted_prefix = settings.TRUSTED_PREFIX
        self.ingestion_date = datetime.now().strftime("%Y-%m-%d")
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
    
    def extract_csv(self, raw_file_path: str):
        """Extract data from CSV file"""
        raw_file = f"{raw_file_path}.csv"
        df = self.datalake.minio.read_csv(raw_file)
        
        if df is None or df.empty:
            logger.error(f"Could not read {raw_file} or file is empty")
            return None
        else:
            logger.info(f"Read {len(df)} rows from {raw_file}")
            return df
    
    def extract_jsonl(self, raw_file_path: str):
        """Extract data from JSONL file"""
        import json
        
        raw_file = f"{raw_file_path}.jsonl"
        try:
            response = self.datalake.minio.client.get_object(
                self.datalake.minio.bucket, 
                raw_file
            )
            
            lines = response.data.decode('utf-8').strip().split('\n')
            data = [json.loads(line) for line in lines if line]
            df = pd.DataFrame(data)
            
            logger.info(f"Read {len(df)} rows from {raw_file}")
            return df
        except Exception as e:
            logger.error(f"Could not read {raw_file}: {e}")
            return None
    
    def set_ingestion_date(self, date_str: str):
        """Set the ingestion date to process"""
        self.ingestion_date = date_str
        logger.info(f"Processing date set to: {date_str}")
        
    def _extract(self) -> Dict[str, Any]:
        """Extract: Read raw data from MinIO"""
        logger.info("Reading raw data from MinIO")
        
        extracted_data = {}
        
        for table_name in get_all_trusted_tables():
            try:
                table_key = table_name.replace('trusted_', '')
                logger.info(f"Reading raw data for {table_key}")
                
                # Read data from RAW layer in MinIO
                raw_file_path = f"raw/ingestion_date={self.ingestion_date}/{table_key}_{self.ingestion_date}"
                
                # Use appropriate extraction method based on data format
                if table_key == 'events':
                    df = self.extract_jsonl(raw_file_path)
                else:
                    df = self.extract_csv(raw_file_path)
                
                if df is not None:
                    extracted_data[table_key] = {
                        'dataframe': df,
                        'trusted_table': table_name
                    }
                else:
                    logger.warning(f"Skipping {table_key} due to read failure")
                
            except Exception as e:
                logger.error(f"Failed to read data for {table_key}: {e}")
                continue
        
        logger.info(f"Extracted {len(extracted_data)} datasets from raw layer")
        return extracted_data
    
    def _transform(self, extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform: Apply data transformations and add ingestion_date"""
        logger.info("Applying data transformations")
        
        transformed_data = {}
        
        for table_key, source_info in extracted_data.items():
            try:
                df = source_info['dataframe']
                
                if 'ingestion_date' not in df.columns:
                    df['ingestion_date'] = self.ingestion_date

                transformed_data[table_key] = {
                    'dataframe': df,
                    'trusted_table': source_info['trusted_table']
                }
                
                logger.debug(f"Transformed {len(df)} rows for {source_info['trusted_table']}")
                
            except Exception as e:
                logger.error(f"Failed to transform data for {table_key}: {e}")
                continue
        
        logger.info(f"Transformed {len(transformed_data)} datasets")
        return transformed_data
    
    def _load(self, transformed_data: Dict[str, Any]) -> ProcessingResult:
        """Load: Write transformed dataframes as parquet files to trusted S3 locations"""
        logger.info("Writing parquet files to trusted S3 locations")
        
        tables_created = []
        total_tables = len(transformed_data)
        successful_loads = 0
        failed_loads = []
        
        # Create schema if needed
        self.datalake.create_database_schema("streampro")
        
        # Write transformed data as parquet files
        for table_key, table_data in transformed_data.items():
            try:
                trusted_table_name = table_data['trusted_table']
                # Get the transformed dataframe
                df = table_data['dataframe']
                
                logger.info(f"Writing {table_key} to parquet ({len(df)} rows)")
                
                # Write to MinIO as parquet
                object_key = f"{self.trusted_prefix}/{table_key}/ingestion_date={self.ingestion_date}/data.parquet"
                success = self.datalake.minio.upload_dataframe(
                    df=df,
                    object_name=object_key,
                    format='parquet'
                )
                
                if success:
                    tables_created.append(trusted_table_name)
                    successful_loads += 1
                    logger.success(f"Wrote parquet file for {trusted_table_name} to {object_key}")
                else:
                    raise Exception("Failed to write to MinIO")
                    
            except Exception as e:
                failed_loads.append({
                    'table': table_data['trusted_table'],
                    'error': str(e)
                })
                logger.error(f"Failed to write {table_data['trusted_table']}: {e}")
        
        # Determine overall success
        success = len(failed_loads) == 0
        
        message = f"Created {successful_loads} trusted parquet tables"
        if failed_loads:
            message += f", {len(failed_loads)} failed"
        
        return ProcessingResult(
            success=success,
            message=message,
            metadata={
                'successful_loads': successful_loads,
                'failed_loads': failed_loads,
                'tables_processed': list(transformed_data.keys()),
                'trusted_prefix': self.trusted_prefix,
                'ingestion_date': self.ingestion_date,
                'format': 'PARQUET',
                'compression': 'SNAPPY',
                'partitioned': True,
                'trino_enabled': True
            },
            rows_processed=total_tables,
            tables_created=tables_created
        )
    
    def _post_process(self, load_result: ProcessingResult) -> None:
        """Post-process: Create external tables pointing to trusted parquet files"""
        logger.info("Creating external tables for trusted parquet files")
        
        if load_result.metadata.get('trino_enabled'):
            logger.info("Trino Trusted Parquet Data Lake Mode:")
            logger.info(f"Pipeline processed: {len(load_result.metadata.get('tables_processed', []))} data sources")
            logger.info(f"Processing date: {load_result.metadata['ingestion_date']}")
            logger.info(f"Target format: {load_result.metadata['format']} with {load_result.metadata['compression']} compression")
            logger.info(f"Partitioned tables for optimized queries")
            
            # Create external tables pointing to the parquet files we just wrote
            logger.info("Creating external tables for trusted parquet data...")
            external_tables_created = []
            
            # Use schema registry to get all trusted tables
            for table_name in get_all_trusted_tables():
                try:
                    # Try to read the parquet file directly from MinIO
                    parquet_path = f"{self.trusted_prefix}/{table_name.replace('trusted_', '')}/ingestion_date={self.ingestion_date}/data.parquet"
                    df = self.datalake.minio.read_parquet(parquet_path)
                    
                    if df is not None and not df.empty:
                        # Get actual columns from the dataframe
                        actual_columns = df.columns.tolist()
                        logger.debug(f"Actual columns in {table_name}: {actual_columns}")
                        
                        # Create a view with the data (limited to first 100 rows for views)
                        values_list = []
                        for _, row in df.head(100).iterrows():
                            # Build value tuple based on actual columns
                            values = []
                            for col in actual_columns:
                                val = row[col]
                                # Handle different data types
                                if pd.isna(val):
                                    values.append('NULL')
                                elif isinstance(val, (int, float)):
                                    values.append(str(val))
                                else:
                                    # Escape single quotes in strings
                                    val_str = str(val).replace("'", "''")
                                    values.append(f"'{val_str}'")
                            values_list.append(f"({', '.join(values)})")
                        
                        if values_list:
                            # Use build_view_ddl from schema registry
                            create_view_sql = build_view_ddl(table_name, values_list)
                            
                            cursor = self.datalake.trino.execute_query(create_view_sql)
                            cursor.fetchall()
                            external_tables_created.append(table_name)
                            logger.success(f"Created view: {table_name}")
                    else:
                        logger.warning(f"No data found for {table_name} in MinIO")
                    
                    try:
                        test_query = f"SELECT COUNT(*) FROM hive.streampro.{table_name}"
                        cursor = self.datalake.trino.execute_query(test_query)
                        count = cursor.fetchall()
                        logger.info(f"Table {table_name} has {count[0][0] if count else 0} rows")
                    except Exception as e:
                        logger.debug(f"Could not query {table_name}: {e}")
                    
                except Exception as e:
                    logger.warning(f"Could not create external table {table_name}: {e}")
            
            if external_tables_created:
                logger.info(f"Created {len(external_tables_created)} external tables for trusted parquet data")
                logger.info("Example trusted layer analytical queries:")
                for table_name in external_tables_created:
                    logger.info(f"   SELECT COUNT(*) FROM hive.streampro.{table_name};")
                    logger.info(f"   SELECT * FROM hive.streampro.{table_name} LIMIT 5;")
                
                load_result.tables_created.extend(external_tables_created)
                load_result.metadata['external_tables_created'] = len(external_tables_created)
            else:
                logger.info("External table creation pending - need actual parquet files in S3")
            
        load_result.metadata['data_lake_ready'] = True
        load_result.metadata['analytics_ready'] = True
    
    def cleanup(self):
        """Cleanup resources"""
        if hasattr(self, 'datalake'):
            self.datalake.close()