import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings


def get_settings(env: str = None) -> 'Settings':
    """Get settings instance for specified environment"""
    if env:
        os.environ["ENV"] = env.lower()
    
    env_name = os.getenv("ENV", "dev").lower()
    env_file_path = Path(f"config/{env_name}.env")
    
    if not env_file_path.exists():
        env_file_path = Path("config/dev.env")
    
    # Create settings with specific env file
    env_file_str = str(env_file_path)
    
    class DynamicSettings(Settings):
        class Config:
            env_file = env_file_str
            case_sensitive = True
    
    return DynamicSettings(ENV=env_name)


class Settings(BaseSettings):
    # Environment
    ENV: Optional[str] = None
    
    # MinIO Configuration
    MINIO_ENDPOINT: Optional[str] = None
    MINIO_ACCESS_KEY: Optional[str] = None
    MINIO_SECRET_KEY: Optional[str] = None
    MINIO_SECURE: Optional[bool] = None
    MINIO_BUCKET: Optional[str] = None
    
    # Storage Layers
    LANDING_PREFIX: Optional[str] = None
    RAW_PREFIX: Optional[str] = None
    TRUSTED_PREFIX: Optional[str] = None
    
    # Trino Configuration
    TRINO_HOST: Optional[str] = None
    TRINO_PORT: Optional[int] = None
    TRINO_USER: Optional[str] = None
    TRINO_CATALOG: Optional[str] = None
    TRINO_SCHEMA: Optional[str] = None
    
    # Logging
    LOG_LEVEL: Optional[str] = None
    
    class Config:
        case_sensitive = True


# Create default settings instance (dev)
settings = get_settings()