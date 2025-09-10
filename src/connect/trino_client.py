import trino
import pandas as pd
from typing import Optional, List, Dict, Any, Union
from pathlib import Path
from loguru import logger

try:
    from src.utils.config import settings
except ImportError:
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent.parent))
    from src.utils.config import settings

from src.connect.minio_client import MinIOClient


class TrinoClient:
    """Trino client for querying data lake - Athena-like functionality"""
    
    def __init__(self, host: str = "localhost", port: int = 8081, catalog: str = "hive", 
                 user: str = "admin", minio_client: Optional[MinIOClient] = None):
        self.host = host
        self.port = port
        self.catalog = catalog
        self.user = user
        self.minio_client = minio_client
        
        # Create Trino connection
        self.conn = trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema="default"
        )
        
        logger.info(f"Connected to Trino at {host}:{port} (catalog: {catalog})")
        
    def execute_query(self, query: str, parameters: Optional[List[Any]] = None) -> trino.dbapi.Cursor:
        """Execute SQL query and return cursor"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, parameters)
            logger.debug(f"Executed Trino query: {query[:100]}...")
            return cursor
        except Exception as e:
            logger.error(f"Error executing Trino query: {e}")
            logger.error(f"Query: {query}")
            raise
    
    def query_to_df(self, query: str, parameters: Optional[List[Any]] = None) -> pd.DataFrame:
        """Execute query and return results as DataFrame"""
        try:
            cursor = self.execute_query(query, parameters)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            # Create DataFrame
            if rows:
                df = pd.DataFrame(rows, columns=columns)
                logger.info(f"Query returned {len(df)} rows, {len(columns)} columns")
                return df
            else:
                # Empty result
                df = pd.DataFrame(columns=columns)
                logger.info("Query returned 0 rows")
                return df
                
        except Exception as e:
            logger.error(f"Error converting query to DataFrame: {e}")
            raise
    
    def create_external_table(self, table_name: str, schema_dict: Dict[str, str], 
                            s3_location: str, file_format: str = "PARQUET") -> bool:
        """Create external table pointing to S3/MinIO location"""
        try:
            # Build column definitions
            columns = ", ".join([f"{col} {dtype}" for col, dtype in schema_dict.items()])
            
            # Create external table SQL
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns}
            )
            WITH (
                external_location = '{s3_location}',
                format = '{file_format}'
            )
            """
            
            self.execute_query(create_sql)
            logger.info(f"Created external table {table_name} pointing to {s3_location}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating external table {table_name}: {e}")
            return False
    
    def create_table_as_select(self, table_name: str, select_query: str, 
                              s3_location: Optional[str] = None) -> bool:
        """Create table as select (CTAS) - stores results in S3/MinIO"""
        try:
            if s3_location:
                # Create external table with specified location
                ctas_sql = f"""
                CREATE TABLE {table_name}
                WITH (
                    external_location = '{s3_location}',
                    format = 'PARQUET'
                )
                AS {select_query}
                """
            else:
                # Create managed table (Trino chooses location)
                ctas_sql = f"""
                CREATE TABLE {table_name}
                WITH (format = 'PARQUET')
                AS {select_query}
                """
            
            self.execute_query(ctas_sql)
            logger.info(f"Created table {table_name} from query")
            return True
            
        except Exception as e:
            logger.error(f"Error creating table as select {table_name}: {e}")
            return False
    
    def drop_table(self, table_name: str) -> bool:
        """Drop table"""
        try:
            self.execute_query(f"DROP TABLE IF EXISTS {table_name}")
            logger.info(f"Dropped table {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error dropping table {table_name}: {e}")
            return False
    
    def table_exists(self, table_name: str, schema: str = "default") -> bool:
        """Check if table exists"""
        try:
            result_df = self.query_to_df(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{schema}' 
                AND table_name = '{table_name}'
            """)
            return len(result_df) > 0
        except Exception as e:
            logger.error(f"Error checking if table exists: {e}")
            return False
    
    def list_tables(self, schema: str = "default") -> List[str]:
        """List all tables in schema"""
        try:
            result_df = self.query_to_df(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{schema}'
                ORDER BY table_name
            """)
            return result_df['table_name'].tolist() if not result_df.empty else []
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []
    
    def get_table_schema(self, table_name: str, schema: str = "default") -> List[Dict[str, str]]:
        """Get table schema information"""
        try:
            result_df = self.query_to_df(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = '{schema}' 
                AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """)
            
            if result_df.empty:
                return []
            
            return [
                {
                    "column": row["column_name"],
                    "type": row["data_type"],
                    "nullable": row["is_nullable"]
                }
                for _, row in result_df.iterrows()
            ]
        except Exception as e:
            logger.error(f"Error getting table schema: {e}")
            return []
    
    def show_partitions(self, table_name: str) -> pd.DataFrame:
        """Show table partitions (if partitioned)"""
        try:
            return self.query_to_df(f"SHOW PARTITIONS {table_name}")
        except Exception as e:
            logger.warning(f"Could not show partitions for {table_name}: {e}")
            return pd.DataFrame()
    
    def analyze_table(self, table_name: str) -> bool:
        """Analyze table statistics for query optimization"""
        try:
            self.execute_query(f"ANALYZE TABLE {table_name}")
            logger.info(f"Analyzed table {table_name}")
            return True
        except Exception as e:
            logger.warning(f"Could not analyze table {table_name}: {e}")
            return False
    
    def close(self):
        """Close Trino connection"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logger.info("Trino connection closed")


class DataLakeManager:
    """Data Lake Manager using Trino + MinIO (Athena + S3 equivalent)"""
    
    def __init__(self):
        self.trino = TrinoClient()
        
        # Try to initialize MinIO client  
        try:
            self.minio = MinIOClient()
            logger.info("MinIO client initialized successfully")
        except Exception as e:
            logger.warning(f"MinIO client initialization failed: {e}. Running in Trino-only mode.")
            self.minio = None
    
    def create_database_schema(self, schema_name: str = "streampro") -> bool:
        """Create database schema for our data lake"""
        try:
            self.trino.execute_query(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            logger.info(f"Created schema {schema_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating schema {schema_name}: {e}")
            return False
    
    def setup_external_tables(self):
        """Set up external tables pointing to S3/MinIO locations"""
        logger.info("Setting up external tables for data lake")
        
        table_configs = {
            "raw_users": {
                "schema": {
                    "user_id": "VARCHAR",
                    "subscription_tier": "VARCHAR", 
                    "signup_date": "DATE",
                    "age_group": "VARCHAR",
                    "gender": "VARCHAR"
                },
                "location": "s3a://streampro-data/raw/users/"
            },
            "raw_videos": {
                "schema": {
                    "video_id": "VARCHAR",
                    "title": "VARCHAR",
                    "genre": "VARCHAR", 
                    "duration_minutes": "INTEGER",
                    "parent_id": "VARCHAR"
                },
                "location": "s3a://streampro-data/raw/videos/"
            },
            "raw_devices": {
                "schema": {
                    "device_id": "VARCHAR",
                    "device_os": "VARCHAR",
                    "app_version": "VARCHAR"
                },
                "location": "s3a://streampro-data/raw/devices/"
            },
            "raw_events": {
                "schema": {
                    "timestamp": "TIMESTAMP",
                    "account_id": "VARCHAR",
                    "user_id": "VARCHAR",
                    "video_id": "VARCHAR",
                    "session_id": "VARCHAR",
                    "event_name": "VARCHAR",
                    "value": "DOUBLE",
                    "device": "VARCHAR",
                    "device_os": "VARCHAR", 
                    "app_version": "VARCHAR",
                    "network_type": "VARCHAR",
                    "country": "VARCHAR"
                },
                "location": "s3a://streampro-data/raw/events/"
            }
        }
        
        # Create external tables
        for table_name, config in table_configs.items():
            self.trino.create_external_table(
                table_name=table_name,
                schema_dict=config["schema"],
                s3_location=config["location"],
                file_format="PARQUET"
            )
    
    def close(self):
        """Close all connections"""
        self.trino.close()
        if self.minio:
            # MinIO client doesn't need explicit close
            pass