from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="flood_monitoring_weekly_pipeline",
    description=(
        "Ingest, transform, load and validate seven days of "
        "Environment Agency flood-monitoring data."
    ),
    default_args=DEFAULT_ARGS,
    start_date=datetime(
        2026,
        9,
        1,
        tzinfo=timezone.utc,
    ),
    schedule_interval="@weekly",
    catchup=False,
    max_active_runs=1,
    tags=["flood-monitoring", "data-engineering"],
) as dag:
    ingest_readings = BashOperator(
        task_id="discover_and_ingest_readings",
        bash_command=(
            "set -euo pipefail; "
            "python src/run_multi_station_ingestion.py"
        ),
        cwd=str(PROJECT_ROOT),
        append_env=True,
        execution_timeout=timedelta(hours=6),
    )

    transform_data = BashOperator(
        task_id="transform_and_deduplicate",
        bash_command=(
            "set -euo pipefail; "
            "python src/run_processed_layer.py"
        ),
        cwd=str(PROJECT_ROOT),
        append_env=True,
        execution_timeout=timedelta(hours=1),
    )

    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command=(
            "set -euo pipefail; "
            'test -n "${DATABASE_URL:-}"; '
            "python src/load_processed_to_postgres.py"
        ),
        cwd=str(PROJECT_ROOT),
        append_env=True,
        execution_timeout=timedelta(hours=1),
    )

    validate_data = BashOperator(
        task_id="validate_data_quality",
        bash_command=(
            "set -euo pipefail; "
            'test -n "${DATABASE_URL:-}"; '
            'duplicate_count=$(psql "$DATABASE_URL" -tAc '
            "\"SELECT COUNT(*) FROM ("
            "SELECT measure_id, reading_timestamp "
            "FROM readings "
            "GROUP BY measure_id, reading_timestamp "
            "HAVING COUNT(*) > 1"
            ") AS duplicates\"); "
            'test "$duplicate_count" -eq 0; '
            'psql "$DATABASE_URL" -v ON_ERROR_STOP=1 '
            "-f sql/validation_queries.sql"
        ),
        cwd=str(PROJECT_ROOT),
        append_env=True,
        execution_timeout=timedelta(minutes=30),
    )

    (
        ingest_readings
        >> transform_data
        >> load_postgres
        >> validate_data
    )
