from typing import Dict, List, Tuple

TRUSTED_SCHEMAS = {
    'trusted_users': {
        'columns': [
            ('user_id', 'VARCHAR'),
            ('signup_date', 'VARCHAR'),
            ('subscription_tier', 'VARCHAR'),
            ('age_group', 'VARCHAR'),
            ('gender', 'VARCHAR'),
            ('ingestion_date', 'VARCHAR')
        ],
        'partition_cols': ['ingestion_date'],
        'location_suffix': 'users'
    },
    
    'trusted_videos': {
        'columns': [
            ('video_id', 'VARCHAR'),
            ('title', 'VARCHAR'),
            ('genre', 'VARCHAR'),
            ('duration_seconds', 'INTEGER'),
            ('patent_id', 'VARCHAR'),
            ('ingestion_date', 'VARCHAR')
        ],
        'partition_cols': ['ingestion_date'],
        'location_suffix': 'videos'
    },
    
    'trusted_devices': {
        'columns': [
            ('device', 'VARCHAR'),
            ('os', 'VARCHAR'),
            ('model', 'VARCHAR'),
            ('os_version', 'DECIMAL(3,1)'),
            ('ingestion_date', 'VARCHAR')
        ],
        'partition_cols': ['ingestion_date'],
        'location_suffix': 'devices'
    },
    
    'trusted_events': {
        'columns': [
            ('timestamp', 'VARCHAR'),
            ('account_id', 'VARCHAR'),
            ('video_id', 'VARCHAR'),
            ('user_id', 'VARCHAR'),
            ('event_name', 'VARCHAR'),
            ('value', 'DECIMAL(2,1)'),
            ('device', 'VARCHAR'),
            ('app_version', 'VARCHAR'),
            ('device_os', 'VARCHAR'),
            ('network_type', 'VARCHAR'),
            ('ip', 'VARCHAR'),
            ('country', 'VARCHAR'),
            ('session_id', 'VARCHAR'),
            ('ingestion_date', 'VARCHAR')
        ],
        'partition_cols': ['ingestion_date'],
        'location_suffix': 'events'
    }
}


def get_trusted_schema(table_name: str) -> Dict:
    """Get schema definition for a trusted table"""
    if table_name not in TRUSTED_SCHEMAS:
        raise ValueError(f"Unknown trusted table: {table_name}")
    return TRUSTED_SCHEMAS[table_name]


def get_all_trusted_tables() -> List[str]:
    """Get list of all trusted table names"""
    return list(TRUSTED_SCHEMAS.keys())


def get_table_columns(table_name: str) -> List[Tuple[str, str]]:
    """Get column definitions for a table"""
    schema = get_trusted_schema(table_name)
    return schema['columns']


def get_table_partition_cols(table_name: str) -> List[str]:
    """Get partition columns for a table"""
    schema = get_trusted_schema(table_name)
    return schema['partition_cols']


def build_table_ddl(table_name: str, s3_location: str) -> str:
    """Build CREATE TABLE DDL for external table"""
    schema = get_trusted_schema(table_name)
    
    # Build column definitions
    columns_def = ', '.join([f"{col} {dtype}" for col, dtype in schema['columns']])
    
    # Build partition clause
    partition_clause = ""
    if schema['partition_cols']:
        partition_clause = f"partitioned_by = ARRAY{schema['partition_cols']}, "
    
    ddl = f"""
    CREATE TABLE IF NOT EXISTS hive.streampro.{table_name} (
        {columns_def}
    )
    WITH (
        {partition_clause}
        external_location = '{s3_location}',
        format = 'PARQUET'
    )
    """
    
    return ddl


def build_view_ddl(table_name: str, data_values: List[str]) -> str:
    """Build CREATE VIEW DDL from data values"""
    schema = get_trusted_schema(table_name)
    
    # Get column names only
    column_names = [col[0] for col in schema['columns']]
    columns_str = ', '.join(column_names)
    
    ddl = f"""
    CREATE OR REPLACE VIEW hive.streampro.{table_name} AS
    SELECT {columns_str} FROM (
        VALUES {', '.join(data_values)}
    ) AS t({columns_str})
    """
    
    return ddl