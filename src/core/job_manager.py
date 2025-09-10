import argparse
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime
import sys
from pathlib import Path
from loguru import logger


class BaseJobManager(ABC):
    """Base job manager class that handles arguments and logging"""
    
    def __init__(self, job_name: str):
        self.job_name = job_name
        self.args: Optional[argparse.Namespace] = None
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
    def setup_args(self) -> argparse.ArgumentParser:
        """Setup command line arguments"""
        parser = argparse.ArgumentParser(description=f"Run {self.job_name}")
        
        # Common arguments
        parser.add_argument("--env", default="dev", choices=["dev", "test", "prod"], 
                          help="Environment (default: dev)")
        parser.add_argument("--ingestion_date", type=str, 
                          help="Ingestion date (YYYY-MM-DD format)")
        parser.add_argument("--source_s3", type=str, 
                          help="Source S3 path")
        parser.add_argument("--target_s3", type=str, 
                          help="Target S3 path")
        parser.add_argument("--debug", action="store_true", 
                          help="Enable debug logging")
        
        # Allow subclasses to add more arguments
        self.add_custom_args(parser)
        
        return parser
    
    def add_custom_args(self, parser: argparse.ArgumentParser):
        """Override this method to add job-specific arguments"""
        pass
    
    def setup_logging(self):
        """Setup logging based on arguments"""
        level = "DEBUG" if self.args.debug else "INFO"
        
        logger.remove()
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> | <level>{message}</level>",
            level=level
        )
        
        # Bind job name to logger
        self.logger = logger.bind(job=self.job_name)
    
    def log_job_start(self):
        """Log job start with all arguments"""
        self.start_time = datetime.now()
        self.logger.info(f"Starting {self.job_name}")
        self.logger.info(f"Arguments:")
        
        for key, value in vars(self.args).items():
            if value is not None:
                self.logger.info(f"{key}: {value}")
    
    def log_job_end(self, success: bool = True):
        """Log job completion"""
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()
        
        if success:
            self.logger.success(f"{self.job_name} completed successfully in {duration:.2f}s")
        else:
            self.logger.error(f"{self.job_name} failed after {duration:.2f}s")
    
    @abstractmethod
    def run(self) -> bool:
        """Override this method to implement job logic"""
        pass
    
    def execute(self):
        """Main execution method"""
        parser = self.setup_args()
        self.args = parser.parse_args()
        
        # Set environment for config loading
        if hasattr(self.args, 'env') and self.args.env:
            import os
            os.environ["ENV"] = self.args.env.lower()
        
        self.setup_logging()
        self.log_job_start()
        
        try:
            success = self.run()
            self.log_job_end(success)
            return 0 if success else 1
            
        except Exception as e:
            self.logger.error(f"💥 {self.job_name} crashed: {e}")
            self.log_job_end(False)
            return 1


class JobManager(BaseJobManager):
    """Complete job manager with run method implemented"""
    
    def __init__(self, job_name: str):
        super().__init__(job_name)
        self.processor = None
    
    def set_processor(self, processor):
        """Set the processor for this job"""
        self.processor = processor
    
    def run(self) -> bool:
        """Run the processor"""
        if not self.processor:
            self.logger.error("No processor set for this job")
            return False
        
        try:
            # Pass arguments to processor if it needs them
            if hasattr(self.processor, 'set_args'):
                self.processor.set_args(self.args)
            
            result = self.processor.run()
            
            if hasattr(result, 'is_success'):
                success = result.is_success
                if success:
                    self.logger.info(f"{result.message}")
                    if hasattr(result, 'metadata') and result.metadata:
                        for key, value in result.metadata.items():
                            if key == 'tables_created' and isinstance(value, list):
                                self.logger.info(f"Tables created: {', '.join(value)}")
                            elif key == 'rows_processed':
                                self.logger.info(f"Rows processed: {value:,}")
                else:
                    self.logger.error(f"Processor failed: {result.error}")
                return success
            else:
                return True
                
        except Exception as e:
            self.logger.error(f"Processor execution failed: {e}")
            return False
        finally:
            if hasattr(self.processor, 'cleanup'):
                self.processor.cleanup()