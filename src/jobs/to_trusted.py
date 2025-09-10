import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.core.simple_job_manager import JobManager
from src.core.raw_to_trusted_processor import RawToTrustedProcessor


def main():
    """Main entry point for trusted data processing job"""
    # Create job manager
    job = JobManager("to_trusted")
    
    # Create and set processor
    processor = RawToTrustedProcessor("raw_to_trusted_processor")
    job.set_processor(processor)
    
    # Execute job
    return job.execute()


if __name__ == "__main__":
    sys.exit(main())