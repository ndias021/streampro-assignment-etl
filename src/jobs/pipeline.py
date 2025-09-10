import sys
import subprocess
from pathlib import Path
from datetime import datetime
from loguru import logger

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.core.job_manager import BaseJobManager


class PipelineManager(BaseJobManager):
    """Pipeline orchestrator for running to_raw and to_trusted jobs sequentially"""
    
    def __init__(self):
        super().__init__("pipeline")
        
    def run(self) -> bool:
        """Run the complete ETL pipeline: to_raw -> to_trusted"""
        
        # Get the base directory for job scripts
        jobs_dir = Path(__file__).parent
        
        common_args = []
        if self.args.env:
            common_args.extend(["--env", self.args.env])
        if self.args.ingestion_date:
            common_args.extend(["--ingestion_date", self.args.ingestion_date])
        if self.args.debug:
            common_args.append("--debug")
            
        # Stage 1: Run to_raw job
        self.logger.info("Starting Stage 1: Landing → Raw")
        to_raw_cmd = ["python", str(jobs_dir / "to_raw.py")] + common_args
        
        try:
            result = subprocess.run(to_raw_cmd, check=True, capture_output=True, text=True)
            self.logger.success("tage 1 completed: Landing → Raw")
            if self.args.debug and result.stdout:
                self.logger.debug(f"to_raw output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Stage 1 failed: {e}")
            if e.stdout:
                self.logger.error(f"stdout: {e.stdout}")
            if e.stderr:
                self.logger.error(f"stderr: {e.stderr}")
            return False
            
        # Stage 2: Run to_trusted job
        self.logger.info("Starting Stage 2: Raw → Trusted")
        to_trusted_cmd = ["python", str(jobs_dir / "to_trusted.py")] + common_args
        
        try:
            result = subprocess.run(to_trusted_cmd, check=True, capture_output=True, text=True)
            self.logger.success("Stage 2 completed: Raw → Trusted")
            if self.args.debug and result.stdout:
                self.logger.debug(f"to_trusted output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Stage 2 failed: {e}")
            if e.stdout:
                self.logger.error(f"stdout: {e.stdout}")
            if e.stderr:
                self.logger.error(f"stderr: {e.stderr}")
            return False
            
        self.logger.success("Pipeline completed successfully!")
        return True


def main():
    pipeline = PipelineManager()
    return pipeline.execute()


if __name__ == "__main__":
    sys.exit(main())