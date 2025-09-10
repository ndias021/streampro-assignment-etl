-- Create databases for Airflow and Hive Metastore
CREATE DATABASE airflow;
CREATE DATABASE metastore;

-- Create users and grant permissions (using MD5 for compatibility)
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE USER metastore WITH PASSWORD 'metastore';

GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
GRANT ALL PRIVILEGES ON DATABASE metastore TO metastore;

-- Grant schema permissions for metastore user
\c metastore
GRANT ALL ON SCHEMA public TO metastore;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO metastore;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO metastore;
GRANT USAGE ON SCHEMA pg_catalog TO metastore;
GRANT SELECT ON ALL TABLES IN SCHEMA information_schema TO metastore;