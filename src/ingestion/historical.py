import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from api.client import FloodMonitoringAPIClient
from ingestion.audit import AuditLogger


# Create a logger for this module.
# The runner configures how these log messages are displayed.
logger = logging.getLogger(__name__)


class HistoricalReadingIngestor:
    """
    Handles historical flood-reading ingestion.

    Responsibilities:
    - Retrieve daily readings from the Environment Agency API.
    - Validate API responses.
    - Combine readings across a date range.
    - Deduplicate readings.
    - Store raw data safely.
    - Record STARTED, SUCCESS and FAILED audit events.
    """

    def __init__(
        self,
        client: FloodMonitoringAPIClient,
        raw_base_path: str = "data/raw/readings",
        audit_logger: AuditLogger | None = None,
    ):
        # API client used to communicate with the source system.
        self.client = client

        # Base directory where raw ingestion files are stored.
        self.raw_base_path = Path(raw_base_path)

        # Allow an AuditLogger to be injected for testing.
        # If one is not supplied, create the default AuditLogger.
        self.audit_logger = (
            audit_logger
            if audit_logger is not None
            else AuditLogger()
        )

    def _fetch_day(
        self,
        station_reference: str,
        reading_date: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """
        Retrieve and validate readings for one station for one day.
        """

        # Request one day's readings from the API.
        data = self.client.get_station_readings(
            station_reference=station_reference,
            reading_date=reading_date,
            limit=limit,
        )

        # Validate the top-level API response.
        if not isinstance(data, dict):
            raise ValueError(
                f"Invalid API response for station "
                f"{station_reference}: expected a dictionary."
            )

        # 'items' is required because it contains the readings.
        # Missing 'items' may indicate an API/schema change.
        if "items" not in data:
            raise ValueError(
                f"Invalid API response for station "
                f"{station_reference}: 'items' field is missing."
            )

        readings = data["items"]

        # Ensure that 'items' has the structure we expect.
        if not isinstance(readings, list):
            raise ValueError(
                f"Invalid API response for station "
                f"{station_reference}: 'items' must be a list."
            )

        # If the response reaches our request limit, we cannot safely
        # assume that we received every reading for that day.
        if len(readings) >= limit:
            raise RuntimeError(
                f"Station {station_reference} reached the "
                f"limit of {limit} readings on {reading_date}. "
                f"Further pagination is required."
            )

        logger.info(
            "Retrieved %s readings for station %s on %s",
            len(readings),
            station_reference,
            reading_date,
        )

        return readings

    def _fetch_date_range(
        self,
        station_reference: str,
        start_date: str,
        end_date: str,
        limit: int,
    ) -> tuple[list[dict[str, Any]], int]:
        """
        Retrieve readings one calendar day at a time.

        The end date is exclusive.

        Example:
        start_date = 2026-09-09
        end_date   = 2026-09-16

        Requests:
        Sep 09, 10, 11, 12, 13, 14 and 15.
        """

        # Convert ISO date strings into Python date objects.
        current_date = datetime.fromisoformat(
            start_date
        ).date()

        final_date = datetime.fromisoformat(
            end_date
        ).date()

        # This will contain readings from every requested day.
        all_readings = []

        api_request_count = 0

        # Process one day at a time until the exclusive end date.
        while current_date < final_date:
            reading_date = current_date.isoformat()

            daily_readings = self._fetch_day(
                station_reference=station_reference,
                reading_date=reading_date,
                limit=limit,
            )

            api_request_count += 1

            # Add this day's readings to the complete collection.
            all_readings.extend(
                daily_readings
            )

            # Move forward by one calendar day.
            current_date += timedelta(days=1)

        return all_readings, api_request_count

    def _deduplicate_readings(
        self,
        readings: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Remove duplicate observations.

        A reading is uniquely identified using:
        measure + dateTime
        """

        unique_readings = {}

        for reading in readings:
            measure = reading.get("measure")
            timestamp = reading.get("dateTime")

            # These fields are required for our uniqueness key.
            if not measure or not timestamp:
                raise ValueError(
                    "Reading is missing 'measure' or 'dateTime'."
                )

            # One measure can have one observation at a given timestamp.
            key = (
                measure,
                timestamp,
            )

            # If the same key occurs again, the latest copy replaces
            # the previous copy rather than creating a duplicate.
            unique_readings[key] = reading

        return list(unique_readings.values())

    def ingest_station(
        self,
        station_reference: str,
        start_date: str,
        end_date: str,
        limit: int = 10000,
    ) -> dict[str, Any]:
        """
        Run the complete historical ingestion process for one station.
        """

        # Generate a unique ID for this ingestion execution.
        # This gives us traceability across logs, audit records and files.
        pipeline_run_id = str(uuid4())

        start_time = time.perf_counter()

        # Record when this ingestion execution started.
        # UTC avoids timezone ambiguity.
        ingestion_timestamp = datetime.now(
            timezone.utc
        ).isoformat()

        # Record STARTED before contacting the API.
        # Even if the API fails immediately, we know the run existed.
        self.audit_logger.write(
            pipeline_run_id=pipeline_run_id,
            status="STARTED",
            details={
                "station_reference": station_reference,
                "requested_start_date": start_date,
                "requested_end_date": end_date,
            },
        )

        try:
            logger.info(
                "Starting historical ingestion for station %s "
                "with pipeline run ID %s",
                station_reference,
                pipeline_run_id,
            )

            # Retrieve the requested historical period.
            readings, api_request_count = self._fetch_date_range(
                station_reference=station_reference,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
            )

            # Remove any duplicate observations before storage.
            readings = self._deduplicate_readings(
                readings
            )

            reading_count = len(readings)

            logger.info(
                "Retrieved %s unique readings for station %s",
                reading_count,
                station_reference,
            )

            # Metadata provides lineage and audit information
            # alongside the raw readings.
            metadata = {
                "pipeline_run_id": pipeline_run_id,
                "ingestion_timestamp": ingestion_timestamp,
                "source_system": (
                    "Environment Agency Flood Monitoring API"
                ),
                "station_reference": station_reference,
                "requested_start_date": start_date,
                "requested_end_date": end_date,
                "reading_count": reading_count,
            }

            # Keep metadata separate from the actual observations.
            raw_output = {
                "metadata": metadata,
                "items": readings,
            }

            # Organise raw data by requested start date and station.
            output_directory = (
                self.raw_base_path
                / f"start_date={start_date}"
                / f"station={station_reference}"
            )

            # Create the directory if it does not already exist.
            output_directory.mkdir(
                parents=True,
                exist_ok=True,
            )

            # Include pipeline_run_id so reruns do not overwrite
            # previous raw ingestion files.
            output_file = (
                output_directory
                / (
                    f"readings_{start_date}_{end_date}_"
                    f"{pipeline_run_id}.json"
                )
            )

            # Write to a temporary file first.
            # The final JSON file should only appear after a successful write.
            temporary_file = output_file.with_suffix(
                ".json.tmp"
            )

            try:
                with temporary_file.open(
                    "w",
                    encoding="utf-8",
                ) as file:
                    json.dump(
                        raw_output,
                        file,
                        indent=2,
                    )

                    # Flush Python's in-memory buffer.
                    file.flush()

                    # Ask the operating system to flush the file
                    # to persistent storage.
                    os.fsync(
                        file.fileno()
                    )

                # Atomically publish the completed file.
                os.replace(
                    temporary_file,
                    output_file,
                )

            except Exception:
                # Remove an incomplete temporary file if writing fails.
                if temporary_file.exists():
                    temporary_file.unlink()

                # Preserve the original exception.
                raise

            logger.info(
                "Raw response written to %s",
                output_file,
            )

            duration_seconds = round(
                time.perf_counter() - start_time,
                2,
           )

            # Only record SUCCESS after the raw file has been
            # completely and successfully published.
            self.audit_logger.write(
                pipeline_run_id=pipeline_run_id,
                status="SUCCESS",
                details={
                    "station_reference": station_reference,
                    "requested_start_date": start_date,
                    "requested_end_date": end_date,
                    "reading_count": reading_count,
                    "api_request_count": api_request_count,
                    "duration_seconds": duration_seconds,
                    "output_file": str(output_file),
                },
            )

            # Return useful information to the caller/orchestrator.
            return {
                "pipeline_run_id": pipeline_run_id,
                "ingestion_timestamp": ingestion_timestamp,
                "station_reference": station_reference,
                "start_date": start_date,
                "end_date": end_date,
                "reading_count": reading_count,
                "api_request_count": api_request_count,
                "duration_seconds": duration_seconds,
                "output_file": str(output_file),
            }

        except Exception as error:

            duration_seconds = round(
                time.perf_counter() - start_time,
                2,
            )

            self.audit_logger.write(
                pipeline_run_id=pipeline_run_id,
                status="FAILED",
                details={
                    "station_reference": station_reference,
                    "requested_start_date": start_date,
                    "requested_end_date": end_date,
                    "duration_seconds": duration_seconds,
                    "error_type": type(error).__name__,
                    "error_message": str(error),
                },
            )
            # logger.exception includes the traceback, which is useful
            # for CloudWatch/Airflow troubleshooting later.
            logger.exception(
                "Historical ingestion failed for station %s "
                "with pipeline run ID %s",
                station_reference,
                pipeline_run_id,
            )

            # Persist information about the failure for auditability.
            self.audit_logger.write(
                pipeline_run_id=pipeline_run_id,
                status="FAILED",
                details={
                    "station_reference": station_reference,
                    "requested_start_date": start_date,
                    "requested_end_date": end_date,
                    "error_type": type(error).__name__,
                    "error_message": str(error),
                },
            )

            # Critical: do not swallow the exception.
            # The future orchestrator must know that this task failed.
            raise