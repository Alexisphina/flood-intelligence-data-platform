from pathlib import Path

import pytest

from ingestion.multi_station import MultiStationIngestor
from run_processed_layer import deduplicate_readings


def test_ingestor_rejects_invalid_worker_count():
    with pytest.raises(
        ValueError,
        match="max_workers must be at least 1",
    ):
        MultiStationIngestor(max_workers=0)


def test_reading_deduplication_rejects_missing_columns():
    records = [{"measure_id": "measure-1"}]

    with pytest.raises(
        ValueError,
        match="Missing columns",
    ):
        deduplicate_readings(records)


def test_reading_deduplication_rejects_measure_station_conflict():
    records = [
        {
            "measure_id": "measure-1",
            "station_reference": "station-1",
            "reading_timestamp": "2026-09-18T10:00:00Z",
            "value": 1.0,
            "ingestion_timestamp": "2026-09-18T10:05:00Z",
        },
        {
            "measure_id": "measure-1",
            "station_reference": "station-2",
            "reading_timestamp": "2026-09-18T10:15:00Z",
            "value": 2.0,
            "ingestion_timestamp": "2026-09-18T10:20:00Z",
        },
    ]

    with pytest.raises(
        ValueError,
        match="multiple stations",
    ):
        deduplicate_readings(records)


def test_airflow_dag_contains_required_tasks():
    dag_file = (
        Path(__file__).resolve().parents[1]
        / "dags"
        / "flood_monitoring_pipeline_dag.py"
    )
    source = dag_file.read_text(encoding="utf-8")

    compile(source, str(dag_file), "exec")

    required_task_ids = [
        "discover_and_ingest_readings",
        "transform_and_deduplicate",
        "load_postgres",
        "validate_data_quality",
    ]

    for task_id in required_task_ids:
        assert f'task_id="{task_id}"' in source

    assert "ingest_readings" in source
    assert "transform_data" in source
    assert "load_postgres" in source
    assert "validate_data" in source
