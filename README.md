# Environment Agency Flood Monitoring Pipeline

This project collects a rolling seven-day window of Environment Agency station readings, retains replayable raw JSON, normalizes station, measure and reading records, removes repeated observations, writes processed Parquet, and upserts curated data into PostgreSQL.

The demonstrated run produced:

| Dataset | Result |
|---|---:|
| Source station records | 5,526 |
| Unique stations stored | 5,516 |
| Measures | 6,781 |
| Readings before deduplication | 7,359 |
| Unique readings | 3,345 |
| Duplicate business keys after processing | 0 |
| Incompatible legacy files quarantined/skipped | 1 |

The repository contains only a small sample of readings. The extraction design enumerates the complete station catalogue; the sample run was kept small enough to review and submit.

## Architecture

```mermaid
flowchart LR
    I[Airflow weekly DAG] --> B
    A[Environment Agency API] --> B[Python extraction]
    B --> C[Raw JSON]
    C --> D[Validation and normalization]
    D --> E[Processed Parquet]
    E --> F[PostgreSQL upsert]
    E --> G[Analysts and data science]
    F --> H[Internal applications]
```

Raw data is immutable and retains API metadata and run identifiers. Processed tables use stable names and typed analytical fields. PostgreSQL enforces referential integrity and the reading business key `(measure_id, reading_timestamp)`, making reruns idempotent.

## Repository structure

```text
dags/flood_monitoring_pipeline_dag.py  Weekly Airflow orchestration
data/raw/                     Immutable API responses
data/processed/stations/      Normalized station Parquet
data/processed/measures/      Normalized measure Parquet
data/processed/readings/      Deduplicated reading Parquet
src/transformation/           Metadata and reading transformations
src/run_processed_layer.py    Raw-to-processed orchestration
src/load_processed_to_postgres.py
schema.sql                    PostgreSQL schema and constraints
sql/validation_queries.sql    Data-quality and analyst queries
sample/sample_readings.csv    Reviewable sample output
tests/                        Automated tests
ASSESSMENT_ANSWERS.md         Answers to the eight questions
```

## Local setup

Python 3.12 is used for local development.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Run the existing raw extraction, then build the processed layer:

```bash
python src/run_processed_layer.py
```

The processed layer selects the newest station snapshot, validates source records, normalizes API fields that can be scalar or list-valued, transforms each reading file once, and retains the latest ingested version for each `(measure_id, reading_timestamp)` key.

## PostgreSQL serving layer

Start the database:

```bash
docker compose up -d postgres
```

Load processed datasets:

```bash
export DATABASE_URL="postgresql://flood:flood@localhost:5432/flood_monitoring"
python src/load_processed_to_postgres.py
```

The load order is stations, measures and readings so foreign keys remain valid. Every load runs in one database transaction and uses upserts, allowing the same processed files to be loaded again safely.

Validate the load:

```bash
docker compose exec postgres \
  psql -U flood -d flood_monitoring -f /dev/stdin \
  < sql/validation_queries.sql
```

## Data quality and idempotency

Required station and reading identifiers are validated before output. API fields that sometimes contain arrays are normalized explicitly, invalid legacy payloads are skipped with a warning, and zero-reading files are recorded separately from failures. Repeated extractions are resolved by keeping the most recently ingested record for each measure and event timestamp; lineage fields remain attached to the retained observation.

Run automated checks with:

```bash
pytest -q
```

The key acceptance checks are:

- required fields are present;
- timestamps parse as UTC;
- values are numeric;
- station and measure keys are unique;
- each measure maps to one station;
- no duplicate `(measure_id, reading_timestamp)` keys remain;
- PostgreSQL row counts reconcile with processed Parquet.

## Failure handling

HTTP calls use timeouts and bounded exponential-backoff retries for transient errors such as 429 and 5xx responses. A failed station or malformed payload is logged with its path and run identifier so it can be retried without losing successful work. Raw responses are retained before transformation, database changes are transactional, and watermarks advance only after a successful load.

## Incremental operation

The initial run requests the previous seven days. Subsequent runs use the API `since` filter from the last successfully committed timestamp with a small overlap window, then rely on the reading primary key to absorb repeats and late corrections. A station that has not advanced for 24 hours is marked stale and alerted; its last watermark is retained so readings are backfilled from that point when the station resumes.

The Environment Agency documentation recommends a single `latest` readings call every 15 minutes for maintaining current values and station-level `since` queries when a complete historical trace is required. The API can also apply implicit result limits, so extraction checks response metadata and pages with `_limit` and `_offset` rather than assuming one response is complete.

## AWS deployment design

The container would run on ECS Fargate, scheduled by EventBridge. Raw and processed layers would live in versioned S3 prefixes, curated tables in RDS PostgreSQL, credentials in Secrets Manager, images in ECR, and logs and custom data-quality metrics in CloudWatch. At larger volume, SQS would partition station work across bounded-concurrency workers and AWS Glue/Spark would replace in-memory processing; Athena could expose Parquet directly to analysts.

## Monitoring

Each run records status, duration, stations attempted/succeeded, records extracted/loaded/rejected, duplicates removed, retries and latest event time. Alerts cover task failure, unexpected zero rows, volume deviation, schema-contract failure, database load failure, and station freshness over 24 hours. These controls detect silent failures where a task exits successfully but delivers incomplete or stale data.

## Security and operational controls

Production credentials are injected at runtime and never committed. RDS is private and restricted by security groups; applications and analysts receive least-privilege read-only access. S3 versioning, run IDs and immutable container tags support audit and targeted replay.

## Source and attribution

API documentation: <https://environment.data.gov.uk/flood-monitoring/doc/reference>

This uses Environment Agency flood and river level data from the real-time data API (Beta), under the Open Government Licence.
