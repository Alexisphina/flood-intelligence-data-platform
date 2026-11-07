"""Load processed Parquet datasets into PostgreSQL using idempotent upserts."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg


def clean(value: Any) -> Any:
    """Convert pandas missing values and timestamps for psycopg."""
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def records(path: Path) -> list[dict[str, Any]]:
    frame = pd.read_parquet(path)
    return [
        {key: clean(value) for key, value in row.items()}
        for row in frame.to_dict(orient="records")
    ]


def load(base_path: Path, database_url: str, schema_path: Path) -> None:
    station_rows = records(base_path / "stations" / "stations.parquet")
    measure_rows = records(base_path / "measures" / "measures.parquet")
    reading_rows = records(base_path / "readings" / "readings.parquet")

    station_columns = [
        "station_reference", "label", "river_name", "catchment_name", "town",
        "latitude", "longitude", "easting", "northing", "status",
        "date_opened", "rloi_id", "wiski_id", "notation", "source_station_url",
    ]
    measure_columns = [
        "measure_id", "station_reference", "parameter", "parameter_name",
        "qualifier", "unit_name", "period_seconds", "source_measure_url",
    ]
    reading_columns = [
        "measure_id", "station_reference", "reading_timestamp", "value",
        "pipeline_run_id", "batch_run_id", "ingestion_timestamp",
    ]

    station_sql = f"""
        INSERT INTO stations ({', '.join(station_columns)})
        VALUES ({', '.join('%s' for _ in station_columns)})
        ON CONFLICT (station_reference) DO UPDATE SET
            label = EXCLUDED.label,
            river_name = EXCLUDED.river_name,
            catchment_name = EXCLUDED.catchment_name,
            town = EXCLUDED.town,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            easting = EXCLUDED.easting,
            northing = EXCLUDED.northing,
            status = EXCLUDED.status,
            date_opened = EXCLUDED.date_opened,
            rloi_id = EXCLUDED.rloi_id,
            wiski_id = EXCLUDED.wiski_id,
            notation = EXCLUDED.notation,
            source_station_url = EXCLUDED.source_station_url,
            updated_at = NOW()
    """
    measure_sql = f"""
        INSERT INTO measures ({', '.join(measure_columns)})
        VALUES ({', '.join('%s' for _ in measure_columns)})
        ON CONFLICT (measure_id) DO UPDATE SET
            station_reference = EXCLUDED.station_reference,
            parameter = EXCLUDED.parameter,
            parameter_name = EXCLUDED.parameter_name,
            qualifier = EXCLUDED.qualifier,
            unit_name = EXCLUDED.unit_name,
            period_seconds = EXCLUDED.period_seconds,
            source_measure_url = EXCLUDED.source_measure_url,
            updated_at = NOW()
    """
    reading_sql = f"""
        INSERT INTO readings ({', '.join(reading_columns)})
        VALUES ({', '.join('%s' for _ in reading_columns)})
        ON CONFLICT (measure_id, reading_timestamp) DO UPDATE SET
            station_reference = EXCLUDED.station_reference,
            value = EXCLUDED.value,
            pipeline_run_id = EXCLUDED.pipeline_run_id,
            batch_run_id = EXCLUDED.batch_run_id,
            ingestion_timestamp = EXCLUDED.ingestion_timestamp,
            loaded_at = NOW()
    """

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(schema_path.read_text(encoding="utf-8"))
            cursor.executemany(
                station_sql,
                [[row.get(column) for column in station_columns] for row in station_rows],
            )
            cursor.executemany(
                measure_sql,
                [[row.get(column) for column in measure_columns] for row in measure_rows],
            )
            cursor.executemany(
                reading_sql,
                [[row.get(column) for column in reading_columns] for row in reading_rows],
            )

    print(
        f"Loaded {len(station_rows):,} stations, "
        f"{len(measure_rows):,} measures and {len(reading_rows):,} readings."
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--processed-path", default="data/processed")
    parser.add_argument("--schema", default="schema.sql")
    args = parser.parse_args()

    database_url = os.environ["DATABASE_URL"]
    load(Path(args.processed_path), database_url, Path(args.schema))


if __name__ == "__main__":
    main()
