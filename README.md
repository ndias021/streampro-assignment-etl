# StreamPro ETL Pipeline

A modern data lakehouse ETL pipeline for analyzing video streaming user engagement patterns using Trino (Athena-like) and MinIO (S3-compatible storage).

## Overview

This ETL pipeline processes streaming platform data to answer key business questions about user retention, engagement, and drop-off patterns. The pipeline uses a modern data lakehouse architecture with Trino as the query engine and MinIO for scalable object storage.

## Architecture

### Data Flow
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Landing   │───▶│     Raw     │───▶│   Trusted   │───▶│  Enriched   │
│  (CSV/JSON) │    │ (Partitioned│    │  (Parquet)  │    │ (Analytics) │
│             │    │  by date)   │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │                   │
       ▼                   ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Local Files │    │MinIO Storage│    │MinIO Storage│    │MinIO Storage│
│             │    │(Date Partns)│    │(Parquet +   │    │(Aggregated) │
│             │    │             │    │ Compression)│    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                            ▲                   ▲                   ▲
                            │                   │                   │
                    ┌───────┴───────────────────┴───────────────────┴───────┐
                    │              Trino Query Engine                       │
                    │        (Athena-like distributed SQL)                  │
                    └───────────────────────────────────────────────────────┘
```

### Technology Stack
- **Storage**: MinIO (S3-compatible object storage)
- **Query Engine**: Trino (distributed SQL query engine)
- **Metadata**: Hive Metastore + PostgreSQL
- **Orchestration**: Simple Python pipeline (`src/jobs/pipeline.py`)
- **Format**: Parquet with Snappy compression
- **Language**: Python 3.10+ with Poetry

### Directory Structure
```
src/
├── connect/          # Trino and MinIO client connections
├── core/            # Job manager and processor classes
├── jobs/            # ETL job entry points
└── catalog/         # Schema definitions and business queries

data/
└── landing/         # Source data files with dates
    ├── events_2025-09-09.jsonl
    ├── users_2025-09-09.csv
    ├── videos_2025-09-09.csv
    └── devices_2025-09-09.csv

config/              # Environment configurations
docker-compose.yml   # Infrastructure services
trino/               # Trino configuration files
```

## Business Questions Addressed

1. **Q1**: What % of new users reach at least 30 seconds of watch_time in their first session?
2. **Q2**: Which video genres drive the highest 2nd-session retention within 3 days?
3. **Q3**: Is there a particular device_os or app_version where drop-off is abnormally high?

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- Poetry

### 1. Start Infrastructure

```bash
# Start all services (Trino, MinIO, PostgreSQL, Hive Metastore)
docker-compose up -d

# Check services are running
docker-compose ps
```

### 2. Install Python Dependencies

```bash
# Install dependencies with Poetry
poetry install

# Activate virtual environment
poetry shell
```

### 3. Verify Data Files

Your landing data should be in `data/landing/` with date suffixes:
```
data/landing/
├── devices_2025-09-09.csv
├── events_2025-09-09.jsonl  
├── users_2025-09-09.csv
└── videos_2025-09-09.csv
```

## How to Run

### Option 1: Complete Pipeline (Recommended)

```bash
# Run the complete pipeline: landing → raw → trusted
poetry run python src/jobs/pipeline.py --env dev --ingestion_date 2025-09-09
```

### Option 2: Individual Jobs

```bash
# Process landing → raw (with date partitioning)
poetry run python src/jobs/to_raw.py --env dev --ingestion_date 2025-09-09

# Process raw → trusted (Parquet conversion)
poetry run python src/jobs/to_trusted.py --env dev --ingestion_date 2025-09-09
```

### Option 3: Interactive Analysis

```bash
# Start Jupyter for data exploration
poetry run jupyter lab

# Or explore via Trino CLI (if available)
trino --server localhost:8081 --catalog hive --schema streampro
```

## Services & Ports

| Service | Port | Purpose |
|---------|------|---------|
| Trino | 8081 | SQL Query Engine (Athena-like) |
| MinIO | 9000/9001 | Object Storage (S3-compatible) |
| PostgreSQL | 5432 | Metadata Storage |
| Hive Metastore | 9083 | Table Metadata |

## Data Schema

### Fact Table: `events_2025-09-09.jsonl` 
- User interaction events (play, pause, stop, etc.)
- ~13K records with user engagement patterns

### Dimension Tables:
- `users_2025-09-09.csv` - User profiles (100 users)
- `videos_2025-09-09.csv` - Video catalog (20 videos) 
- `devices_2025-09-09.csv` - Device types (5 devices)

## Pipeline Stages

### Stage 1: Landing → Raw (Date Partitioned)
- Uploads files from local landing to MinIO object storage
- Creates date-based partitions: `raw/ingestion_date=2025-09-09/`
- Extracts date from filename automatically
- Creates Trino external table definitions

**Processor**: `LandingToRawProcessor`
**Job**: `python src/jobs/to_raw.py`

### Stage 2: Raw → Trusted (Parquet Transformation)
- Queries raw data via Trino SQL
- Applies data quality rules and business logic
- Converts to compressed Parquet format with optimal partitioning
- Creates trusted tables for analytics

**Processor**: `RawToTrustedProcessor`
**Job**: `python src/jobs/to_trusted.py`

### Stage 3: Trusted → Enriched (Business Analytics)
- Aggregates trusted data for business questions
- Creates session-level and user journey metrics
- Generates performance analytics by device/genre

**Job**: `python src/jobs/to_enriched.py`

## Key Tables Created

### Raw Layer (Partitioned by Date)
- `raw_events`: Raw event data partitioned by `ingestion_date`
- `raw_users`: User dimension data with date partitions
- `raw_videos`: Video catalog with date partitions
- `raw_devices`: Device reference data with date partitions

### Trusted Layer (Parquet Format)
- `trusted_events`: Cleaned event stream with business logic applied
- `trusted_users`: Validated user profiles with data quality checks
- `trusted_videos`: Clean video catalog with derived attributes
- `trusted_devices`: Standardized device information

### Enriched Layer (Business Analytics)
- `user_sessions`: Session-level metrics and engagement analysis
- `user_journey`: User lifecycle and retention analysis
- `genre_performance`: Video genre performance metrics
- `device_analytics`: Device/app version performance analysis

## Configuration

Edit `config.py` or environment-specific files in `config/`:
- `config/dev.env` - Development settings
- `config/prod.env` - Production settings

Key settings:
```python
MINIO_ENDPOINT = "localhost:9000" 
TRINO_HOST = "localhost"
TRINO_PORT = 8081
LOCAL_DATA_PATH = "data/landing"
```

## File Processing Logic

Files are automatically processed based on naming convention:
- `table_YYYY-MM-DD.ext` → Extract date and table type
- Partitioned in MinIO as: `raw/ingestion_date=YYYY-MM-DD/`
- Converted to Parquet in: `trusted/table_name/`

## Troubleshooting

### Services not starting
```bash
# Check Docker logs
docker-compose logs trino
docker-compose logs minio

# Restart services
docker-compose restart
```

### Python import errors
```bash
# Install in development mode
poetry install --with dev

# Or set PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
```

### MinIO connection issues
- Check MinIO is accessible: http://localhost:9001
- Default credentials: minioadmin/minioadmin
- Verify bucket `streampro-data` exists

### Trino query failures
- Verify Hive Metastore is running
- Check table metadata in information_schema
- Test connection: `curl http://localhost:8081/v1/info`

## Development

### Adding New Processors
1. Extend `BaseProcessor` class
2. Implement ETL methods: `_extract()`, `_transform()`, `_load()`
3. Create job script in `src/jobs/`

### Schema Changes
1. Update processor schema definitions
2. Modify Trino table DDL
3. Update business query logic

For questions or issues, check the logs in each service container.