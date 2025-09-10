import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.core.job_manager import JobManager
from src.core.landing_to_raw_processor import LandingToRawProcessor


def main():
    """Main entry point for raw data processing job"""
    # Create job manager
    job = JobManager("to_raw")
    
    # Create and set processor
    processor = LandingToRawProcessor("landing_to_raw_processor")
    job.set_processor(processor)
    
    # Execute job
    return job.execute()


if __name__ == "__main__":
    sys.exit(main())