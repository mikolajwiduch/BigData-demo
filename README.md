# Multi-Cloud Financial ETL Pipeline

Automated multi-cloud (AWS + Azure) ETL pipeline for processing financial data. This project orchestrates the retrieval, transformation, structured storage, and monitoring of market data using modern DevOps tools.

## Architecture & Components

### 1. Extraction (`src/extract.py`)
Fetches daily stock prices (e.g., CDR.WA, PKO.WA, VWCE.DE, ^SPX) over a 30-day period using public APIs (`yfinance`).
- **Data Lake (AWS S3)**: Serves as the raw data landing zone. The script structures objects into logical partitions (`/raw_stock_data/date=YYYY-MM-DD/`) and uploads them as JSON payloads. This step interacts with AWS using a dedicated IAM Service Account with tightly scoped permissions (Bucket Read/Write).

### 2. Transformation & Load (`src/transform_load.py`)
Acts as the main processing engine responsible for validating and sanitizing the ingested datasets.
- Pulls the latest JSON extract from the AWS S3 Bucket.
- Uses `pandas` to forward-fill missing data points (crucial for maintaining continuous timelines across exchange holidays and weekends).
- Computes essential rolling metrics (7-day and 30-day moving averages).
- Connects to **Azure Database for PostgreSQL Flexible Server** to execute targeted UPSERTS (`ON CONFLICT DO NOTHING`) into the primary fact table (`daily_prices_fact`).

### 3. Auditing & Logging
Database-level operation tracking. The transformation stage is wrapped in a discrete auditing context that logs pipeline state.
- **`etl_audit_log` Table**: Collects vital telemetry regarding the execution run, including execution ID, total successful row loads, error strings (if any), and full execution duration.

### 4. Infrastructure as Code (Terraform)
Deploys and maintains the required cloud footprint.
- **Azure Resource Group & DB Server**: Stands up the core database with specific configurations (`B1ms` compute tier) designed for the processing load while mitigating configuration drifts (lifecycle exclusions on zone placements).
- **AWS S3 & IAM Policies**: Provisions the bucket, the dedicated IAM user for the script, and tightly scoped policies enforcing minimum privileges.

### 5. Contanerization & Orchestration (`Dockerfile`, `run_pipeline.sh`)
Provides an isolated runtime context to neutralize host environment disparities.
- **Docker Engine**: Encapsulates Python 3.12, system-level dependencies for psycopg2 (libpq-dev), and the project's logic into a single image.
- **Shell Wrapper**: Sequential execution wrapper that orchestrates the invocation of `extract.py` followed strictly by `transform_load.py`.

### 6. Monitoring & Telemetry (Grafana)
Provides deep visibility into both the business telemetry and operational health of the pipeline.
- Provisioned via `docker-compose`. Connects transparently to the Azure PostgreSQL instance on boot.
- Auto-loads a core dashboard utilizing a Base 10 Logarithmic scale for normalizing metric outliers (e.g., ^SPX values vs CDR.WA).
- Centralizes pipeline errors based on the `etl_audit_log` queries.

## Execution Guide

### Prerequisite Setup
1. Define Cloud Credentials in Terraform's `.tfvars` or host CLI (`az login` / `aws configure`).
2. Deploy the Infrastructure footprint:
```bash
cd terraform
terraform init
terraform apply -auto-approve
```
3. Read the provisioned AWS IAM secrets outputted natively by Terraform.

### Local Application Run
1. Rename `.env.example` to `.env`.
2. Populate the parameters mapping to `DB_PASSWORD`, `DB_HOST`, `S3_BUCKET_NAME`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`.
3. Apply schema definition to the newly created database:
```bash
docker run -it --rm -v $(pwd)/sql:/sql postgres:16 psql -h [DB_HOST] -U [DB_USER] -d [DB_NAME] -f /sql/schema.sql
```
4. Build the runtime and execute the pipeline payload:
```bash
docker build -t etl-pipeline-demo .
docker run --rm --env-file .env etl-pipeline-demo
```

### Telemetry / Ops
Start Grafana locally to view the executed loads:
```bash
docker compose up -d
```
Access via `http://localhost:3000` (`admin` / `admin`).
