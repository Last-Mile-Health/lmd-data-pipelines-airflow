# LMD Data Pipelines — Airflow

Config-driven ETL platform built on Apache Airflow, AWS Glue Spark, and Redshift Serverless. Ingests data from multiple sources (CSV, DHIS2, REST APIs, Kobo Toolbox), processes it through a three-layer data lake (Raw → Processed → Curated), and loads into country-specific Redshift databases.

### DAG Task Flow

Each pipeline generates an Airflow DAG with these tasks:

```
resolve_load_params → ingest_to_raw → raw_to_processed (Glue)
    → processed_to_curated (Glue) → crawl_curated (Glue Crawler)
    → load_to_redshift → update_watermark → run_quality_checks
```

| Task | What it does |
|------|-------------|
| `resolve_load_params` | Read watermark from DynamoDB, determine incremental boundaries |
| `ingest_to_raw` | Pull data from source API → write JSON to S3 Raw layer. **Skips** if 0 records. |
| `trigger_raw_to_processed` | Glue Spark job: flatten, clean columns, deduplicate, custom SQL |
| `trigger_processed_to_curated` | Glue Spark job: business logic transforms via custom SQL |
| `crawl_curated` | Glue Crawler: register Parquet schema in Glue Data Catalog |
| `load_to_redshift` | Auto-create table from Glue catalog + COPY/MERGE into Redshift |
| `update_watermark` | Save new high-watermark to DynamoDB |
| `run_quality_checks` | Row count, null checks, custom SQL assertions against Redshift |

## Architecture

```
┌──────────────┐
│  React App   │──── REST API ────▶ Airflow Webserver (port 8080)
└──────────────┘                    │
                                    ▼
                          ┌──────────────────┐
                          │  DAG Factory      │  Reads config/pipelines/*.yml
                          │  (generic_etl_dag)│  Generates one DAG per YAML
                          └────────┬─────────┘
                                   │
          ┌────────────────────────┼────────────────────────┐
          ▼                        ▼                        ▼
   ┌─────────────┐         ┌─────────────┐         ┌─────────────┐
   │ CSV Ingest  │         │ DHIS2 Ingest│         │ Kobo Ingest │
   │ API Ingest  │         │             │         │             │
   └──────┬──────┘         └──────┬──────┘         └──────┬──────┘
          └────────────────────────┼────────────────────────┘
                                   ▼
                    ┌──────────────────────────┐
                    │  RAW (S3 / JSON)          │  Raw data, partitioned by date
                    └────────────┬─────────────┘
                                 ▼
                    ┌──────────────────────────┐
                    │  Glue Spark: raw_to_     │  Flatten, clean, deduplicate
                    │  processed + custom SQL   │  sql/raw_to_processed/*.sql
                    └────────────┬─────────────┘
                                 ▼
                    ┌──────────────────────────┐
                    │  PROCESSED (S3 / Parquet) │  Cleaned, schema-enforced
                    └────────────┬─────────────┘
                                 ▼
                    ┌──────────────────────────┐
                    │  Glue Spark: processed_  │  Business logic transforms
                    │  to_curated + custom SQL  │  sql/processed_to_curated/*.sql
                    └────────────┬─────────────┘
                                 ▼
                    ┌──────────────────────────┐
                    │  CURATED (S3 / Parquet)   │  Business-ready
                    └────────────┬─────────────┘
                                 ▼
                    ┌──────────────────────────┐
                    │  Glue Crawler             │  Auto-detect schema from Parquet
                    └────────────┬─────────────┘
                                 ▼
                    ┌──────────────────────────┐
                    │  Redshift Serverless      │  Auto-create table + COPY/MERGE
                    │  (auth via Secrets Mgr)   │  Per-pipeline database
                    └──────────────────────────┘
```

## Project Structure

```
├── docker-compose.yml              Local Airflow (Postgres + optional LocalStack)
├── Dockerfile                      Airflow 2.9.3 / Python 3.11
├── requirements.txt                Python dependencies
├── .env.example                    Environment variable template
├── Makefile                        Common commands
│
├── config/pipelines/               One YAML per pipeline (auto-discovered)
│   ├── lib_ifi_pipeline.yml        IFI pipeline config
│   ├── lib_dhis2_pipeline.yml      DHIS2 pipeline config
│   └── _template.yml               Copy this to add a new pipeline
│
├── dags/
│   ├── generic_etl_dag.py          DAG factory — generates DAGs from YAMLs
│   ├── operators/
│   │   ├── ingest/
│   │   │   ├── kobo_ingest.py      Kobo Toolbox API ingestor
│   │   │   ├── dhis2_ingest.py     DHIS2 API ingestor
│   │   │   ├── api_ingest.py       Generic REST API ingestor
│   │   │   └── csv_ingest.py       S3 CSV file pickup ingestor
│   │   └── redshift_load.py        Redshift loader (replace/append/merge)
│   └── utils/
│       ├── cdc.py                  CDC watermark tracking (DynamoDB)
│       ├── config_loader.py        YAML config + env config loader
│       ├── s3_helpers.py           S3 read/write utilities
│       └── quality.py             Post-load data quality checks
│
├── glue_jobs/                      PySpark scripts (deployed to S3)
│   ├── raw_to_processed.py         Generic fallback: JSON → clean Parquet + custom SQL
│   ├── processed_to_curated.py     Generic fallback: Parquet → business logic + custom SQL
│   ├── lib_ifi_pipeline/           Per-pipeline Glue scripts
│   │   ├── raw_to_processed.py
│   │   └── processed_to_curated.py
│   └── lib_dhis2_pipeline/
│       ├── raw_to_processed.py
│       └── processed_to_curated.py
│
├── sql/                            Custom SQL transformations
│   ├── raw_to_processed/
│   │   ├── lib_ifi_pipeline_clean.sql       IFI cleaning SQL
│   │   ├── lib_dhis2_pipeline_clean.sql     DHIS2 cleaning SQL
│   │   └── _template.sql                    Template for new pipelines
│   ├── processed_to_curated/
│   │   ├── lib_ifi_pipeline_transform.sql   IFI business logic SQL
│   │   ├── lib_dhis2_pipeline_transform.sql DHIS2 business logic SQL
│   │   └── _template.sql                    Template for new pipelines
│   └── redshift/                   DDL, migrations, quality check SQL
│
├── plugins/api/
│   └── pipeline_api.py             Custom REST API for React management app
│
├── infrastructure/                 CDK for Airflow-specific AWS resources
│   ├── app.py                      CDK entry point
│   ├── airflow_stack.py            Glue jobs + IAM roles per pipeline
│   ├── cdk.json
│   └── requirements.txt
│
└── tests/
    ├── unit/
    ├── integration/
    └── dags/
```

## Prerequisites

- Docker & Docker Compose
- AWS CLI configured with credentials
- AWS CDK (`npm install -g aws-cdk`) for deploying Glue jobs
- Python 3.11+

### Existing AWS Resources (from lmd-data-pipelines CDK stack)

These must already exist — this project connects to them, it does not create them:

| Resource | Name Pattern | Notes |
|----------|-------------|-------|
| S3 Buckets | `{prefix}-raw`, `{prefix}-processed`, `{prefix}-curated`, `{prefix}-assets` | Created by CDK |
| DynamoDB | `{prefix}-pipeline-metadata` | Watermark + run metadata |
| Redshift Serverless | Secret: `REDSHIFT_SECRET_NAME` in `.env` | Contains workgroup, host, credentials |
| Redshift IAM Role | `{prefix}-redshift-spectrum-role` | Created by CDK, for Spectrum + S3 access |
| Glue Catalog | Database: `{prefix}` (underscored) | Auto-populated by crawlers |
| Glue IAM Role | `{prefix}-glue-role` | Created by CDK, used by jobs + crawlers |
| Secrets Manager | Kobo, DHIS2, Redshift secrets | Referenced by name in `.env` |

Where `{prefix}` = `{LMD_PROJECT_CODE}-{LMD_ENVIRONMENT}` (e.g., `lmd-dp-airflow-v1-dev`).

## Quick Start

### 1. Configure environment

```bash
cp .env.example .env
# Edit .env — fill in AWS credentials and verify resource names
```

### 2. Build and start Airflow locally

```bash
make build
make up
```

Airflow UI: [http://localhost:8080](http://localhost:8080) (username: `admin`, password: `admin`)

### 3. Start with LocalStack (offline development)

```bash
make up-local
```

This starts LocalStack alongside Airflow, providing local S3, DynamoDB, and Secrets Manager.

### 4. Deploy Glue scripts and SQL to S3

```bash
make deploy-assets
```

This syncs `glue_jobs/` and `sql/` to the S3 assets bucket so Glue jobs can reference them.

### 5. Deploy Glue jobs via CDK

```bash
cd infrastructure
pip install -r requirements.txt
cdk deploy --context env=dev
```

This creates the Glue job definitions and IAM roles in AWS.

## Adding a New Pipeline

Adding a pipeline requires no code changes — just config files:

### Step 1: Create the pipeline YAML

```bash
cp config/pipelines/_template.yml config/pipelines/my_pipeline.yml
```

Edit `my_pipeline.yml` and configure:

- **source**: Data source type and connection details
- **ingestion.mode**: `full` (pull everything) or `incremental` (watermark-based)
- **processed.custom_sql**: Path to cleaning SQL (optional)
- **curated.custom_sql**: Path to business logic SQL (optional)
- **redshift.load_mode**: `replace`, `append`, or `merge`
- **schedule.cron**: Cron expression for automated runs

### Step 2: Add custom SQL (optional)

```bash
# Cleaning SQL (Raw → Processed)
cp sql/raw_to_processed/_template.sql sql/raw_to_processed/my_pipeline.sql

# Business logic SQL (Processed → Curated)
cp sql/processed_to_curated/_template.sql sql/processed_to_curated/my_pipeline.sql
```

### Step 3: Register the Glue jobs

Add the pipeline name to `infrastructure/app.py`:

```python
PIPELINE_NAMES = ["lib_ifi_pipeline", "lib_dhis2_pipeline", "my_pipeline"]
```

Then deploy:

```bash
make deploy-assets
cd infrastructure && cdk deploy --context env=dev
```

### Step 4: Enable the DAG

The DAG (`etl_my_pipeline`) auto-appears in Airflow. Unpause it in the UI or via API:

```bash
curl -X PATCH http://localhost:8080/api/v1/dags/etl_my_pipeline \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{"is_paused": false}'
```

## Data Source Types

### CSV (`type: csv`)

Picks up CSV files from an S3 prefix. For incremental mode, only processes files modified since the last run.

```yaml
source:
  type: csv
  config:
    s3_prefix: "uploads/facility_list/"
    delimiter: ","
    encoding: "utf-8"
```

### DHIS2 (`type: dhis2`)

Pulls from DHIS2 analytics, dataValueSets, events, or tracked entity endpoints.

```yaml
source:
  type: dhis2
  secret_name: lmd-dev-dhis2-secrets    # DHIS2_USERNAME, DHIS2_PASSWORD, DHIS2_BASE_URL
  config:
    endpoint: "/api/analytics"
    dimensions:
      - "dx:INDICATOR_ID"
      - "pe:LAST_12_MONTHS"
      - "ou:ORG_UNIT_ID"
    page_size: 1000
```

### Generic REST API (`type: api`)

Supports offset, cursor, and page-based pagination.

```yaml
source:
  type: api
  secret_name: my-api-secrets           # API_TOKEN or API_KEY
  config:
    base_url: "https://api.example.com"
    endpoint: "/v1/data"
    method: GET
    pagination:
      type: offset                       # offset | cursor | page | none
      page_size: 1000
    data_path: "response.data.items"     # JSON path to records
```

### Kobo Toolbox (`type: kobo_api`)

Pulls form submissions with pagination and incremental filtering.

```yaml
source:
  type: kobo_api
  secret_name: lmd-dev-kobo-secrets      # KOBO_API_TOKEN, KOBO_API_URL
  config:
    form_id: "aGFR9xP8QKuZqLmNoPqRsT"
    export_format: json
    default_country: liberia
```

## Load Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `replace` | TRUNCATE table, then COPY | Full refresh, small tables |
| `append` | COPY directly (additive) | Event/log data, append-only |
| `merge` | COPY to staging → DELETE matching → INSERT | Upsert by primary key |

Configure in the pipeline YAML:

```yaml
redshift:
  load_mode: merge
  merge_keys: ["_id"]
```

### Per-Pipeline Redshift Database

Each pipeline targets a **country-specific Redshift database** configured in its YAML. This allows multi-country deployments on the same Redshift Serverless workgroup:

```yaml
# config/pipelines/lib_ifi_pipeline.yml
redshift:
  database: liberia           # country-specific database
  schema: public
  table: ifi_data_v1
  load_mode: merge
  merge_keys: ["_id"]
```

```yaml
# config/pipelines/chw_registry.yml
redshift:
  database: sierra_leone      # different country, different database
  schema: public
  table: chw_registry
  load_mode: replace
```

If `database` is omitted, the global `REDSHIFT_DATABASE` from `.env` is used as fallback.

### Redshift Authentication (Secrets Manager)

The Redshift Data API authenticates via **AWS Secrets Manager**, not hardcoded credentials. The secret (configured as `REDSHIFT_SECRET_NAME` in `.env`) contains:

| Key | Description |
|-----|-------------|
| `workgroupName` | Redshift Serverless workgroup name |
| `host` | Endpoint hostname |
| `port` | Connection port |
| `username` | Database user |
| `password` | Database password |
| `engine` | `redshift` |
| `namespaceName` | Serverless namespace |

The pipeline automatically:
1. Fetches the secret ARN and values from Secrets Manager
2. Extracts `workgroupName` for the Data API
3. Passes `SecretArn` to every `execute_statement` call

No workgroup name or credentials in `.env` — everything comes from the secret.

### Auto-Create Tables (Glue Crawler)

Redshift tables are **created automatically** from the Parquet file schema. No DDL needed:

1. After the curated Glue job writes Parquet, a **Glue Crawler** runs on the curated S3 path
2. The crawler registers/updates the table schema in the **Glue Data Catalog**
3. Before COPY, `_ensure_table_exists` checks if the Redshift table exists
4. If not, it reads the schema from the Glue catalog, maps types (Glue → Redshift), and runs `CREATE TABLE`

**Type mapping:**

| Glue Type | Redshift Type |
|-----------|--------------|
| `string` | `VARCHAR(65535)` |
| `int` | `INTEGER` |
| `bigint` / `long` | `BIGINT` |
| `double` | `DOUBLE PRECISION` |
| `float` | `REAL` |
| `boolean` | `BOOLEAN` |
| `date` | `DATE` |
| `timestamp` | `TIMESTAMP` |
| `decimal(p,s)` | `DECIMAL(p,s)` |
| `array<>` / `struct<>` / `map<>` | `SUPER` |

This means if the source adds new fields, the Glue Crawler picks them up and the Redshift table gets created with the updated schema (for new tables). For existing tables with new columns, you would need to ALTER TABLE.

## Email Notifications

Pipelines can send email alerts on start, failure, and retry. Configure in the pipeline YAML:

```yaml
# config/pipelines/lib_ifi_pipeline.yml
alerts:
  email_on_start: true          # send email when pipeline begins
  email_on_failure: true         # send email on any task failure
  email_on_retry: false          # send email on task retry (can be noisy)
  email_recipients:
    - data-team@lmh.org
    - jdunyo@lastmilehealth.org
```

### SMTP Setup

Add SMTP credentials to `.env` (uses AWS SES by default):

```bash
SMTP_HOST=email-smtp.us-east-1.amazonaws.com
SMTP_PORT=587
SMTP_USER=<SES SMTP username>        # create in AWS SES console → SMTP Settings
SMTP_PASSWORD=<SES SMTP password>
SMTP_MAIL_FROM=.org       # must be verified in SES
```

### Failure email includes

- Pipeline name and failed task name
- Execution date and try number
- Full error message
- Direct link to task logs

### Disabling notifications

Set `email_recipients: []` or omit the `alerts` section entirely.

## Change Data Capture (CDC)

Incremental pipelines track a high-watermark in DynamoDB to avoid re-processing data:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  DynamoDB   │────▶│  Ingest     │────▶│  Process &  │────▶│  Update     │
│  Watermark  │     │  (new only) │     │  Load       │     │  Watermark  │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
       ▲                                                           │
       └───────────────────────────────────────────────────────────┘
```

### How it works

| Step | What happens | Example |
|------|-------------|---------|
| 1. Read watermark | `get_watermark()` from DynamoDB | `_submission_time = "2026-02-15T10:30:00Z"` |
| 2. Ingest | Only fetch records where `incremental_key > watermark` | Kobo API: `query={"_submission_time": {"$gt": "2026-02-15T10:30:00Z"}}` |
| 3. Process | Glue jobs process only this new batch | Raw → Processed → Curated |
| 4. Load | `merge` mode upserts into Redshift by `_id` | DELETE matching + INSERT new |
| 5. Save watermark | Persist `max(_submission_time)` from this batch | `"2026-02-17T08:00:00Z"` |

### First run

No watermark exists → `start_after = None` → pulls **all records** from the source.

### No new data

If 0 records are returned (watermark is up to date), the `ingest_to_raw` task is marked as **skipped** and all downstream tasks are automatically skipped. The DAG succeeds (no error).

### Override at runtime

Trigger a full refresh for an incremental pipeline (ignores watermark):

```bash
# Via Airflow API
curl -X POST http://localhost:8080/api/v1/dags/etl_lib_ifi_pipeline/dagRuns \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d '{"conf": {"mode_override": "full", "country": "liberia"}}'
```

```bash
# Via Airflow UI
# Trigger DAG → Configuration JSON → {"mode_override": "full"}
```

## Custom SQL Transformations

### Raw → Processed (`sql/raw_to_processed/`)

Runs inside the Glue Spark job after flattening and column cleaning. The raw data is available as a temp view called `source_data`.

```sql
-- sql/raw_to_processed/my_pipeline.sql
SELECT
    id,
    LOWER(TRIM(name)) AS name,
    CAST(value AS DOUBLE) AS value,
    COALESCE(category, 'UNCATEGORIZED') AS category
FROM source_data
WHERE id IS NOT NULL
```

### Processed → Curated (`sql/processed_to_curated/`)

Supports sequential transformation steps using the `-- transform_` convention:

```sql
-- transform_add_timestamps
SELECT *,
       TO_TIMESTAMP(created_at) AS created_timestamp
FROM __TABLE__;

-- transform_business_logic
SELECT
    id,
    name,
    created_timestamp,
    CASE WHEN value > 100 THEN 'HIGH' ELSE 'LOW' END AS tier
FROM __TABLE__;
```

Each step reads from `__TABLE__` (the previous step's result) and produces a new `__TABLE__`.

## React Management API

The React app interacts with two APIs:

### Airflow Built-in API (`/api/v1/`)

| Action | Method | Endpoint |
|--------|--------|----------|
| List pipelines | `GET` | `/api/v1/dags` |
| Get pipeline status | `GET` | `/api/v1/dags/{dag_id}` |
| Trigger a run | `POST` | `/api/v1/dags/{dag_id}/dagRuns` |
| List runs | `GET` | `/api/v1/dags/{dag_id}/dagRuns` |
| Get task statuses | `GET` | `/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances` |
| Get task logs | `GET` | `/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try}` |
| Pause/Unpause | `PATCH` | `/api/v1/dags/{dag_id}` |

### Custom Pipeline API (`/api/v1/pipeline/`)

| Action | Method | Endpoint |
|--------|--------|----------|
| Health check | `GET` | `/api/v1/pipeline/health` |
| List pipeline configs | `GET` | `/api/v1/pipeline/configs` |
| Get pipeline config | `GET` | `/api/v1/pipeline/configs/{name}` |
| Get watermark & history | `GET` | `/api/v1/pipeline/metadata/{name}` |

### Trigger a run with overrides (from React)

```json
POST /api/v1/dags/etl_lib_ifi_pipeline/dagRuns
{
  "conf": {
    "mode_override": "full",
    "country": "liberia",
    "date_from": "2024-01-01"
  },
  "note": "Manual full refresh triggered by admin"
}
```

## Scheduling

Schedules are defined per pipeline in the YAML config:

| Pipeline | Cron | Description |
|----------|------|-------------|
| lib_ifi_pipeline | `0 6 * * *` | Daily at 6 AM UTC |
| lib_dhis2_pipeline | `0 7 * * *` | Daily at 7 AM UTC |

On-demand runs can be triggered via Airflow UI, CLI, or the REST API.

## Makefile Commands

| Command | Description |
|---------|-------------|
| `make up` | Start Airflow locally |
| `make up-local` | Start with LocalStack for offline dev |
| `make down` | Stop all containers |
| `make build` | Rebuild Docker image |
| `make restart` | Restart webserver and scheduler |
| `make logs` | Tail scheduler + webserver logs |
| `make shell` | Shell into the webserver container |
| `make test` | Run tests inside Docker |
| `make test-local` | Run tests locally |
| `make lint` | Lint all Python code |
| `make deploy-glue` | Upload Glue scripts to S3 |
| `make deploy-sql` | Upload SQL files to S3 |
| `make deploy-assets` | Upload both Glue scripts and SQL |
| `make deploy-mwaa` | Deploy DAGs + plugins + config to MWAA S3 bucket |
| `make deploy-mwaa-infra` | Deploy MWAA CDK stack (VPC, IAM, MWAA environment) |
| `make deploy-all` | Deploy all CDK stacks + sync code to MWAA |
| `make clean` | Stop containers and remove volumes |

## Deploying to AWS (MWAA)

Amazon Managed Workflows for Apache Airflow (MWAA) runs your DAGs in a fully managed environment — no Docker, no EC2, no Airflow maintenance.

### What MWAA Creates

The CDK stack (`mwaa_stack.py`) provisions:

| Resource | Purpose |
|----------|---------|
| S3 bucket (`{prefix}-mwaa`) | Stores DAGs, plugins.zip, requirements.txt |
| VPC + 2 private subnets | Network isolation for MWAA workers |
| NAT Gateway | Allows workers to reach AWS APIs and internet |
| Security group | Self-referencing for MWAA internal traffic |
| IAM execution role | Access to S3, Glue, Redshift, Secrets Manager, DynamoDB, SES |
| MWAA environment | Airflow 2.9.2, `mw1.small` (configurable) |

### Step 1: Deploy Infrastructure (first time only)

```bash
# Set environment
export LMD_ENVIRONMENT=dev
export LMD_PROJECT_CODE=lmd-dp-airflow-v1

# Deploy pipeline resources (S3 buckets, Glue jobs, DynamoDB)
cd infrastructure
pip install -r requirements.txt
cdk bootstrap   # first time only
cdk deploy --context env=dev --all
```

This creates both stacks:
- `lmd-dp-airflow-v1-dev` — S3 buckets, Glue jobs, DynamoDB, IAM
- `lmd-dp-airflow-v1-dev-mwaa` — VPC, MWAA environment, execution role

MWAA takes ~25 minutes to create.

### Step 2: Deploy Code

```bash
# From project root
export LMD_ENVIRONMENT=dev
export LMD_PROJECT_CODE=lmd-dp-airflow-v1

make deploy-mwaa
```

This uploads to the MWAA S3 bucket:
- `dags/` → `s3://{prefix}-mwaa/dags/`
- `config/` → `s3://{prefix}-mwaa/dags/config/` (so DAGs can read pipeline YAMLs)
- `plugins.zip` → `s3://{prefix}-mwaa/plugins.zip`
- `requirements.txt` → `s3://{prefix}-mwaa/requirements.txt`
- Glue scripts + SQL → `s3://{prefix}-assets/` (via `deploy-assets`)

MWAA picks up DAG changes within ~30 seconds. Plugin and requirements changes trigger an environment update (~5 minutes).

### Step 3: Set Environment Variables

In the [MWAA console](https://console.aws.amazon.com/mwaa/) → your environment → Edit → Airflow configuration options, add:

```
LMD_ENVIRONMENT = dev
LMD_PROJECT_CODE = lmd-dp-airflow-v1
REDSHIFT_SECRET_NAME = lmd-20-dev
KOBO_SECRET_NAME = lmd-dev-kobo-secrets
DHIS2_SECRET_NAME = lmd-dev-dhis2-secrets
```

Or set them in the CDK stack's `airflow_configuration_options` for infrastructure-as-code.

**Note:** AWS credentials are NOT needed — MWAA uses the execution role automatically.

### Step 4: Access the UI

```bash
# Get the MWAA webserver URL
aws mwaa get-environment --name lmd-dp-airflow-v1-dev-mwaa \
  --query 'Environment.WebserverUrl' --output text
```

Open the URL in your browser. MWAA uses IAM authentication — you need `airflow:CreateWebLoginToken` permission.

### Deploying Updates

```bash
# Code changes (DAGs, config, SQL, plugins)
make deploy-mwaa

# Infrastructure changes (new Glue jobs, new pipelines)
cd infrastructure && cdk deploy --context env=dev --all

# Everything
make deploy-all
```

### Production Deployment

```bash
export LMD_ENVIRONMENT=prod
export LMD_PROJECT_CODE=lmd-dp-airflow-v1

# Deploy all infrastructure
cd infrastructure && cdk deploy --context env=prod --all

# Deploy code
make deploy-mwaa
```

For production, consider updating `mwaa_stack.py`:
- `environment_class`: `mw1.medium` or `mw1.large` for more capacity
- `max_workers`: increase based on number of concurrent pipelines
- `webserver_access_mode`: `PRIVATE_ONLY` if behind a VPN
