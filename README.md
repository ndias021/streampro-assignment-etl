# StreamPro ETL Pipeline

A simple data engineering pipeline that processes streaming video analytics data through landing -> raw -> trusted layers.

## How to Setup & Run

### 1. Start Infrastructure
```bash
docker-compose up -d
```
This starts:
- MinIO (S3-like storage) at http://localhost:9000
- MinIO Console at http://localhost:9001 (admin/minioadmin)

### 2. Install Dependencies
```bash
poetry install
poetry shell
```

### 3. Run the Pipeline

**Process data from landing � raw:**
```bash
poetry run python src/core/to_raw.py --env dev --ingestion_date 2025-09-09
```

**Process data from raw � trusted:**
```bash
poetry run python src/core/to_trusted.py --env dev --ingestion_date 2025-09-09
```

**Or run the full pipeline:**
```bash
poetry run python src/core/pipeline.py --env dev --ingestion_date 2025-09-09
```

## Sample Data

I added sample data in the `data/` folder to simulate real-world scenarios:
- `data/users_2025-09-09.csv`
- `data/videos_2025-09-09.csv` 
- `data/devices_2025-09-09.csv`
- `data/events_2025-09-09.jsonl`

This emulates data arriving at the landing layer, then being processed through raw and trusted layers.

## Data Analysis Results

See `src/notebooks/analysis.ipynb` for full analysis. Key findings:

### Q1: What % of new users reach at least 30 seconds of watch_time in their first session?
**Answer: 1.0%**
- Only 1 out of 100 users (user_78) reached 30+ seconds in first session
- 97 users had some watch time, but most had very short sessions

### Q2: Which video genres drive the highest 2nd-session retention within 3 days?
**Answer: Comedy**
- All genres have 100% binary retention (everyone comes back)
- But Comedy drives highest quality engagement:
  - 80.6 seconds average subsequent watch time
  - 7.4 average subsequent sessions 
  - 595.9 engagement quality score

### Q3: Is there a particular device_os or app_version where drop-off is abnormally high?
**Answer: iOS + 2.0.1**
- There is abnormally high drop-off in: iOS + 2.0.1

## Architecture

```
Landing Layer  -> Raw Layer              -> Trusted Layer           -> Analytics
CSV/JSON files -> CSV/JSON files (as-is) -> Processed Parquet files -> DuckDB queries

Landing: files from `./data are dropped into MinIO when service starts
Raw: Parquet files are copied from Landing as is
Trusted: Parquet files are created from Raw files
Analytics: DuckDB queries are run on Trusted Parquet files in MinIO S3
```

- **MinIO**: Emulates AWS S3 for data lake storage
- **DuckDB**: Emulates AWS Athena for fast analytics
- **Parquet**: Columnar storage format for efficient queries

## Next Steps

### Option 1: On-Premise Setup
- **Orchestration**: Kubernetes + Docker + Airflow
- **Storage**: MinIO (S3-compatible)
- **Analytics**: DuckDB 
- **Monitoring**: Grafana + Loki
- **Logs**: PostgreSQL

### Option 2: Cloud Setup (AWS)
- **Orchestration**: Step Functions
- **Compute**: Lambda + Glue
- **Storage**: S3 
- **Analytics**: Athena
- **Logs**: DynamoDB
- **Secrets**: Secrets Manager
- **SQL Tables**: RDS (if needed)

Both setups would provide production-grade scalability, monitoring, and reliability.