import os
from io import BytesIO
from typing import List, Optional, Union
from pathlib import Path
import pandas as pd
from minio import Minio
from minio.error import S3Error
from loguru import logger
try:
    from src.utils.config import settings
except ImportError:
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent.parent))
    from src.utils.config import settings


class MinIOClient:
    def __init__(self):
        self.client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE
        )
        self.bucket = settings.MINIO_BUCKET
        self._ensure_bucket()
    
    def _ensure_bucket(self):
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                logger.info(f"Created bucket: {self.bucket}")
        except S3Error as e:
            logger.error(f"Error creating bucket: {e}")
            raise
    
    def upload_file(self, local_path: Union[str, Path], object_name: str) -> bool:
        try:
            self.client.fput_object(self.bucket, object_name, str(local_path))
            logger.info(f"Uploaded {local_path} to {object_name}")
            return True
        except S3Error as e:
            logger.error(f"Error uploading {local_path}: {e}")
            return False
    
    def upload_dataframe(self, df: pd.DataFrame, object_name: str, format: str = "parquet") -> bool:
        try:
            if format.lower() == "parquet":
                buffer = BytesIO()
                df.to_parquet(buffer, index=False)
                buffer.seek(0)
                self.client.put_object(
                    self.bucket, 
                    object_name, 
                    buffer, 
                    length=buffer.getbuffer().nbytes,
                    content_type="application/octet-stream"
                )
            elif format.lower() == "csv":
                csv_buffer = BytesIO()
                df.to_csv(csv_buffer, index=False)
                csv_buffer.seek(0)
                self.client.put_object(
                    self.bucket,
                    object_name,
                    csv_buffer,
                    length=csv_buffer.getbuffer().nbytes,
                    content_type="text/csv"
                )
            logger.info(f"Uploaded dataframe to {object_name} as {format}")
            return True
        except Exception as e:
            logger.error(f"Error uploading dataframe: {e}")
            return False
    
    def download_file(self, object_name: str, local_path: Union[str, Path]) -> bool:
        try:
            self.client.fget_object(self.bucket, object_name, str(local_path))
            logger.info(f"Downloaded {object_name} to {local_path}")
            return True
        except S3Error as e:
            logger.error(f"Error downloading {object_name}: {e}")
            return False
    
    def read_parquet(self, object_name: str) -> Optional[pd.DataFrame]:
        try:
            response = self.client.get_object(self.bucket, object_name)
            df = pd.read_parquet(BytesIO(response.data))
            logger.info(f"Read parquet file: {object_name}")
            return df
        except Exception as e:
            logger.error(f"Error reading parquet {object_name}: {e}")
            return None
    
    def read_csv(self, object_name: str) -> Optional[pd.DataFrame]:
        try:
            response = self.client.get_object(self.bucket, object_name)
            df = pd.read_csv(BytesIO(response.data))
            logger.info(f"Read CSV file: {object_name}")
            return df
        except Exception as e:
            logger.error(f"Error reading CSV {object_name}: {e}")
            return None
    
    def list_objects(self, prefix: str = "") -> List[str]:
        try:
            objects = self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing objects: {e}")
            return []
    
    def copy_object(self, source_key: str, target_key: str) -> bool:
        """Copy object within the same bucket"""
        try:
            from minio.commonconfig import CopySource
            copy_source = CopySource(self.bucket, source_key)
            self.client.copy_object(self.bucket, target_key, copy_source)
            logger.info(f"Copied {source_key} -> {target_key}")
            return True
        except S3Error as e:
            logger.error(f"Error copying {source_key} to {target_key}: {e}")
            return False

    def delete_object(self, object_name: str) -> bool:
        try:
            self.client.remove_object(self.bucket, object_name)
            logger.info(f"Deleted object: {object_name}")
            return True
        except S3Error as e:
            logger.error(f"Error deleting {object_name}: {e}")
            return False
    
    def get_object_url(self, object_name: str) -> str:
        return f"s3://{self.bucket}/{object_name}"