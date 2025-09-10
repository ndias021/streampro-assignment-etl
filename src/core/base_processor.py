from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime
from loguru import logger

from enum import Enum


class JobStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class JobResult:
    """Simple job result"""
    job_id: str
    status: JobStatus
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    message: Optional[str] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    @property
    def is_success(self) -> bool:
        return self.status == JobStatus.SUCCESS


@dataclass
class ProcessingResult:
    """Result of a processing operation"""
    success: bool
    message: str
    metadata: Dict[str, Any]
    rows_processed: int = 0
    tables_created: List[str] = None
    
    def __post_init__(self):
        if self.tables_created is None:
            self.tables_created = []


class BaseProcessor(ABC):
    """Abstract base class for data processors following ETL pattern"""
    
    def __init__(self, processor_id: str, description: str = ""):
        self.processor_id = processor_id
        self.description = description
        self._start_time: Optional[datetime] = None
        self._end_time: Optional[datetime] = None
        self.args = None  # Will be set by job manager if needed
    
    def set_args(self, args):
        """Set arguments from job manager"""
        self.args = args
    
    def run(self) -> JobResult:
        """Template method that orchestrates the ETL process"""
        logger.info(f"Starting processor: {self.processor_id}")
        self._start_time = datetime.now()
        
        try:
            # Template method pattern - define the algorithm
            self._pre_process()
            extracted_data = self._extract()
            transformed_data = self._transform(extracted_data)
            load_result = self._load(transformed_data)
            self._post_process(load_result)
            
            self._end_time = datetime.now()
            duration = (self._end_time - self._start_time).total_seconds()
            
            logger.success(f"Processor {self.processor_id} completed in {duration:.2f}s")
            
            return JobResult(
                job_id=self.processor_id,
                status=JobStatus.SUCCESS,
                start_time=self._start_time,
                end_time=self._end_time,
                duration_seconds=duration,
                message=load_result.message,
                metadata={
                    **load_result.metadata,
                    "rows_processed": load_result.rows_processed,
                    "tables_created": load_result.tables_created
                }
            )
            
        except Exception as e:
            self._end_time = datetime.now()
            duration = (self._end_time - self._start_time).total_seconds() if self._start_time else 0
            
            logger.error(f"Processor {self.processor_id} failed after {duration:.2f}s: {e}")
            
            return JobResult(
                job_id=self.processor_id,
                status=JobStatus.FAILED,
                start_time=self._start_time,
                end_time=self._end_time,
                duration_seconds=duration,
                error=str(e)
            )
    
    def _pre_process(self) -> None:
        """Hook for pre-processing setup (optional override)"""
        pass
    
    @abstractmethod
    def _extract(self) -> Any:
        """Extract data from source - must be implemented by subclasses"""
        pass
    
    @abstractmethod
    def _transform(self, extracted_data: Any) -> Any:
        """Transform the extracted data - must be implemented by subclasses"""
        pass
    
    @abstractmethod
    def _load(self, transformed_data: Any) -> ProcessingResult:
        """Load the transformed data - must be implemented by subclasses"""
        pass
    
    def _post_process(self, load_result: ProcessingResult) -> None:
        """Hook for post-processing cleanup (optional override)"""
        pass
    
    def cleanup(self):
        """Cleanup resources"""
        pass

