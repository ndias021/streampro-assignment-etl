import duckdb
import pandas as pd
from typing import Optional, List, Dict, Any, Union
from pathlib import Path
from loguru import logger
import tempfile
import os

try:
    from src.utils.config import settings
except ImportError:
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent.parent))
    from src.utils.config import settings

from src.connect.minio_client import MinIOClient


class DuckDBClient:
    """DuckDB client for querying data lake - Athena-like functionality with better performance"""
    
    def __init__(self, database: str = ":memory:", minio_client: Optional[MinIOClient] = None):
        """Initialize DuckDB client
        
        Args:
            database: Path to DuckDB database file, or ":memory:" for in-memory database
            minio_client: MinIO client for S3-compatible storage access
        """
        self.database = database
        self.minio_client = minio_client
        
        # Create DuckDB connection
        self.conn = duckdb.connect(database)
        
        # Configure S3-compatible storage (MinIO) if available
        if minio_client:
            self._configure_s3_access()
        
        # Install and load required extensions
        self._setup_extensions()
        
        logger.info(f"Connected to DuckDB (database: {database})")
        
    def _setup_extensions(self):
        """Install and load required DuckDB extensions"""
        try:
            # Install and load httpfs for S3 access
            self.conn.execute("INSTALL httpfs;")
            self.conn.execute("LOAD httpfs;")
            
            # Install and load parquet support (usually built-in)
            self.conn.execute("INSTALL parquet;")
            self.conn.execute("LOAD parquet;")
            
            logger.info("DuckDB extensions loaded successfully")
        except Exception as e:
            logger.warning(f"Some DuckDB extensions may not be available: {e}")
    
    def _configure_s3_access(self):
        """Configure DuckDB for S3-compatible storage access"""
        if not self.minio_client:
            return
            
        try:
            # Get MinIO settings from the client
            from src.utils.config import settings
            
            # Set S3 endpoint and credentials for MinIO
            self.conn.execute(f"""
                SET s3_endpoint = '{settings.MINIO_ENDPOINT}';
            """)
            
            self.conn.execute(f"""
                SET s3_access_key_id = '{settings.MINIO_ACCESS_KEY}';
            """)
            
            self.conn.execute(f"""
                SET s3_secret_access_key = '{settings.MINIO_SECRET_KEY}';
            """)
            
            # Configure SSL based on MinIO settings
            ssl_setting = "false" if settings.MINIO_ENDPOINT.startswith("localhost") else "true"
            self.conn.execute(f"""
                SET s3_use_ssl = {ssl_setting};
            """)
            
            # Set region (MinIO doesn't use regions, but DuckDB may require it)
            self.conn.execute("SET s3_region = 'us-east-1';")
            
            logger.info("DuckDB configured for S3-compatible storage access")
            
        except Exception as e:
            logger.warning(f"Could not configure S3 access: {e}")
    
    def execute_query(self, query: str, parameters: Optional[List[Any]] = None):
        """Execute SQL query and return result"""
        try:
            if parameters:
                result = self.conn.execute(query, parameters)
            else:
                result = self.conn.execute(query)
            logger.debug(f"Executed DuckDB query: {query[:100]}...")
            return result
        except Exception as e:
            logger.error(f"Error executing DuckDB query: {e}")
            logger.error(f"Query: {query}")
            raise
    
    def query_to_df(self, query: str, parameters: Optional[List[Any]] = None) -> pd.DataFrame:
        """Execute query and return results as DataFrame"""
        try:
            result = self.execute_query(query, parameters)
            df = result.df()
            logger.info(f"Query returned {len(df)} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.error(f"Error converting query to DataFrame: {e}")
            raise
    
    def create_table_from_parquet(self, table_name: str, parquet_path: str) -> bool:
        """Create table from parquet file(s)"""
        try:
            # Drop table if exists
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Create table from parquet
            create_sql = f"""
                CREATE TABLE {table_name} AS 
                SELECT * FROM read_parquet('{parquet_path}')
            """
            
            self.execute_query(create_sql)
            logger.info(f"Created table {table_name} from parquet: {parquet_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating table {table_name} from parquet: {e}")
            return False
    
    def create_view_from_parquet(self, view_name: str, parquet_path: str) -> bool:
        """Create view pointing to parquet file(s) - more memory efficient"""
        try:
            # Drop view if exists
            self.conn.execute(f"DROP VIEW IF EXISTS {view_name}")
            
            # Create view from parquet
            create_sql = f"""
                CREATE VIEW {view_name} AS 
                SELECT * FROM read_parquet('{parquet_path}')
            """
            
            self.execute_query(create_sql)
            logger.info(f"Created view {view_name} from parquet: {parquet_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating view {view_name} from parquet: {e}")
            return False
    
    def create_table_as_select(self, table_name: str, select_query: str) -> bool:
        """Create table as select (CTAS)"""
        try:
            ctas_sql = f"CREATE TABLE {table_name} AS {select_query}"
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
    
    def drop_view(self, view_name: str) -> bool:
        """Drop view"""
        try:
            self.execute_query(f"DROP VIEW IF EXISTS {view_name}")
            logger.info(f"Dropped view {view_name}")
            return True
        except Exception as e:
            logger.error(f"Error dropping view {view_name}: {e}")
            return False
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        try:
            result_df = self.query_to_df("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name = ?
            """, [table_name])
            return len(result_df) > 0
        except Exception as e:
            logger.error(f"Error checking if table exists: {e}")
            return False
    
    def view_exists(self, view_name: str) -> bool:
        """Check if view exists"""
        try:
            result_df = self.query_to_df("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_name = ? AND table_type = 'VIEW'
            """, [view_name])
            return len(result_df) > 0
        except Exception as e:
            logger.error(f"Error checking if view exists: {e}")
            return False
    
    def list_tables(self) -> List[str]:
        """List all tables"""
        try:
            result_df = self.query_to_df("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            return result_df['table_name'].tolist() if not result_df.empty else []
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []
    
    def list_views(self) -> List[str]:
        """List all views"""
        try:
            result_df = self.query_to_df("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_type = 'VIEW'
                ORDER BY table_name
            """)
            return result_df['table_name'].tolist() if not result_df.empty else []
        except Exception as e:
            logger.error(f"Error listing views: {e}")
            return []
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """Get table schema information"""
        try:
            result_df = self.query_to_df("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = ?
                ORDER BY ordinal_position
            """, [table_name])
            
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
    
    def analyze_table(self, table_name: str) -> bool:
        """Analyze table statistics for query optimization"""
        try:
            self.execute_query(f"ANALYZE {table_name}")
            logger.info(f"Analyzed table {table_name}")
            return True
        except Exception as e:
            logger.warning(f"Could not analyze table {table_name}: {e}")
            return False
    
    def close(self):
        """Close DuckDB connection"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logger.info("DuckDB connection closed")


class DataLakeManager:
    """Data Lake Manager using DuckDB + MinIO (better than Athena + S3)"""
    
    def __init__(self, database: str = ":memory:"):
        """Initialize Data Lake Manager
        
        Args:
            database: Path to DuckDB database file, or ":memory:" for in-memory database
        """
        # Initialize MinIO client
        try:
            self.minio = MinIOClient()
            logger.info("MinIO client initialized successfully")
        except Exception as e:
            logger.warning(f"MinIO client initialization failed: {e}")
            self.minio = None
        
        # Initialize DuckDB client with MinIO configuration
        self.duckdb = DuckDBClient(database=database, minio_client=self.minio)
    
    def setup_trusted_tables_from_parquet(self, ingestion_date: str = "2025-09-09"):
        """Set up trusted tables by reading parquet files from MinIO"""
        logger.info("Setting up trusted tables from parquet files")
        
        if not self.minio:
            logger.error("MinIO client not available - cannot setup tables")
            return False
        
        # Table configurations - read from MinIO and create tables directly
        table_configs = {
            "trusted_users": f"trusted/users/ingestion_date={ingestion_date}/data.parquet",
            "trusted_videos": f"trusted/videos/ingestion_date={ingestion_date}/data.parquet", 
            "trusted_devices": f"trusted/devices/ingestion_date={ingestion_date}/data.parquet",
            "trusted_events": f"trusted/events/ingestion_date={ingestion_date}/data.parquet"
        }
        
        success_count = 0
        for table_name, minio_path in table_configs.items():
            try:
                # Read parquet file from MinIO
                df = self.minio.read_parquet(minio_path)
                
                if df is not None and not df.empty:
                    # Create table in DuckDB from the DataFrame
                    self.duckdb.drop_table(table_name)  # Clean up any existing table
                    self.duckdb.drop_view(table_name)   # Clean up any existing view
                    
                    # Create table directly from DataFrame (DuckDB is very efficient with this)
                    table_creation_sql = f"CREATE TABLE {table_name} AS SELECT * FROM df"
                    self.duckdb.execute_query(table_creation_sql)
                    
                    success_count += 1
                    logger.info(f"✅ {table_name}: {len(df):,} rows loaded into DuckDB")
                else:
                    logger.warning(f"No data found for {table_name} at {minio_path}")
                    
            except Exception as e:
                logger.warning(f"Could not setup {table_name}: {e}")
        
        logger.info(f"Successfully set up {success_count}/{len(table_configs)} trusted tables")
        return success_count == len(table_configs)
    
    def query_parquet_directly(self, parquet_path: str, query: str = "SELECT * FROM parquet_scan") -> pd.DataFrame:
        """Query parquet file directly without creating table/view"""
        try:
            # Replace parquet_scan placeholder with actual path
            final_query = query.replace("parquet_scan", f"read_parquet('{parquet_path}')")
            return self.duckdb.query_to_df(final_query)
        except Exception as e:
            logger.error(f"Error querying parquet directly: {e}")
            return pd.DataFrame()
    
    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """Get comprehensive table statistics"""
        try:
            stats = {}
            
            # Row count
            count_df = self.duckdb.query_to_df(f"SELECT COUNT(*) as row_count FROM {table_name}")
            stats['row_count'] = count_df['row_count'].iloc[0] if not count_df.empty else 0
            
            # Column count  
            schema = self.duckdb.get_table_schema(table_name)
            stats['column_count'] = len(schema)
            stats['columns'] = [col['column'] for col in schema]
            
            # Sample data
            sample_df = self.duckdb.query_to_df(f"SELECT * FROM {table_name} LIMIT 5")
            stats['sample_data'] = sample_df.to_dict('records') if not sample_df.empty else []
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting table stats: {e}")
            return {}
    
    def close(self):
        """Close all connections"""
        if hasattr(self, 'duckdb'):
            self.duckdb.close()
        # MinIO client doesn't need explicit close