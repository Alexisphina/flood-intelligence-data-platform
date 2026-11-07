import pytest

from run_processed_layer import (
    deduplicate_readings,
    deduplicate_stations,
)


def test_deduplicate_stations_keeps_last_record():
    records = [
        {"station_reference": "E7190", "label": "Old"},
        {"station_reference": "E7190", "label": "New"},
        {"station_reference": "E2900", "label": "Other"},
    ]

    result = deduplicate_stations(records)

    assert len(result) == 2
    assert result[0]["station_reference"] == "E7190"
    assert result[0]["label"] == "New"


def test_deduplicate_stations_rejects_blank_reference():
    records = [{"station_reference": " ", "label": "Invalid"}]

    with pytest.raises(
        ValueError,
        match="cannot be null or blank",
    ):
        deduplicate_stations(records)


def test_deduplicate_readings_keeps_latest_record():
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
            "station_reference": "station-1",
            "reading_timestamp": "2026-09-18T10:00:00Z",
            "value": 2.0,
            "ingestion_timestamp": "2026-09-18T10:10:00Z",
        },
    ]

    result = deduplicate_readings(records)

    assert len(result) == 1
    assert result[0]["value"] == 2.0
