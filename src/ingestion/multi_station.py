import logging
import time
import json
import os
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from api.client import FloodMonitoringAPIClient
from ingestion.historical import HistoricalReadingIngestor


logger = logging.getLogger(__name__)


class MultiStationIngestor:
    """
    Coordinates historical ingestion across multiple monitoring
    stations using bounded concurrency.
    """

    def __init__(
        self,
        max_workers: int = 2,
    ):
        # We must always have at least one worker.
        if max_workers < 1:
            raise ValueError(
                "max_workers must be at least 1."
            )

        # Controls the maximum number of stations that can
        # be processed concurrently.
        self.max_workers = max_workers

    def discover_stations(
        self,
    ) -> list[str]:
        """
        Dynamically retrieve monitoring stations from the
        Environment Agency API.

        Returns a list of unique station references.
        """

        logger.info(
            "Discovering monitoring stations from source API"
        )

        # Discovery uses its own API client.
        client = FloodMonitoringAPIClient()

        data = client.get_stations()

        # Validate the API response before using it.
        if not isinstance(data, dict):
            raise ValueError(
                "Invalid station API response: "
                "expected a dictionary."
            )

        if "items" not in data:
            raise ValueError(
                "Invalid station API response: "
                "'items' field is missing."
            )

        stations = data["items"]

        if not isinstance(stations, list):
            raise ValueError(
                "Invalid station API response: "
                "'items' must be a list."
            )

        # Preserve a snapshot of the station metadata response.
        # This gives the transformation layer access to descriptive
        # station and measure attributes without calling the API again.
        ingestion_timestamp = datetime.now(
            timezone.utc
        )

        snapshot_directory = Path(
            "data/raw/stations"
        )

        snapshot_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        snapshot_filename = (
            "stations_"
            f"{ingestion_timestamp.strftime('%Y%m%dT%H%M%SZ')}"
            ".json"
        )

        snapshot_file = (
            snapshot_directory / snapshot_filename
        )

        temp_file = Path(
            f"{snapshot_file}.tmp"
        )

        try:
            with temp_file.open(
                "w",
                encoding="utf-8",
            ) as file:

                json.dump(
                    data,
                    file,
                    indent=2,
                )

                file.flush()
                os.fsync(file.fileno())

            os.replace(
                temp_file,
                snapshot_file,
            )

        except Exception:

            if temp_file.exists():
                temp_file.unlink()

            raise

        logger.info(
            "Station metadata snapshot written to %s",
            snapshot_file,
        )

        station_references = []

        # Extract stationReference from each station record.
        for station in stations:

            if not isinstance(station, dict):
                raise ValueError(
                    "Invalid station record: "
                    "expected a dictionary."
                )

            station_reference = station.get(
                "stationReference"
            )

            # Do not attempt ingestion for an unusable
            # station record.
            if not station_reference:
                logger.warning(
                    "Skipping station record with no "
                    "stationReference"
                )

                continue

            station_references.append(
                station_reference
            )

        # Remove duplicate station references while
        # preserving the API's original order.
        unique_station_references = list(
            dict.fromkeys(station_references)
        )

        logger.info(
            "Discovered %s unique monitoring stations",
            len(unique_station_references),
        )

        return unique_station_references

    def _ingest_one_station(
        self,
        station_reference: str,
        start_date: str,
        end_date: str,
        batch_run_id: str,
    ) -> dict[str, Any]:
        """
        Process one station inside a worker thread.
        """

        # Each worker creates its own API client/session.
        # We do not share the same requests.Session
        # between concurrent workers.
        client = FloodMonitoringAPIClient()

        ingestor = HistoricalReadingIngestor(
            client=client,
        )

        # Pass the parent batch ID into the individual
        # station ingestion.
        return ingestor.ingest_station(
            station_reference=station_reference,
            start_date=start_date,
            end_date=end_date,
            batch_run_id=batch_run_id,
        )

    def ingest_stations(
        self,
        station_references: list[str],
        start_date: str,
        end_date: str,
    ) -> dict[str, Any]:
        """
        Process multiple stations using bounded concurrency.
        """

        # One parent identifier represents this entire batch.
        batch_run_id = str(uuid4())

        # Record when the overall batch started.
        batch_start_timestamp = datetime.now(
            timezone.utc
        ).isoformat()

        # Start measuring total batch duration.
        start_time = time.perf_counter()

        successful_stations = []
        failed_stations = []

        logger.info(
            "Starting batch %s for %s stations "
            "using %s workers",
            batch_run_id,
            len(station_references),
            self.max_workers,
        )

        # max_workers limits how many station ingestions
        # can execute at the same time.
        with ThreadPoolExecutor(
            max_workers=self.max_workers
        ) as executor:

            # Submit every station as a future task.
            # Only max_workers tasks execute concurrently.
            future_to_station = {
                executor.submit(
                    self._ingest_one_station,
                    station_reference,
                    start_date,
                    end_date,
                    batch_run_id,
                ): station_reference
                for station_reference in station_references
            }

            # Process station results as soon as each
            # worker completes.
            for future in as_completed(
                future_to_station
            ):

                station_reference = (
                    future_to_station[future]
                )

                try:
                    result = future.result()

                    successful_stations.append(
                        result
                    )

                    logger.info(
                        "Station %s completed successfully",
                        station_reference,
                    )

                except Exception as error:
                    # One failed station does not prevent the
                    # remaining stations from completing.
                    failed_stations.append(
                        {
                            "station_reference": (
                                station_reference
                            ),
                            "error_type": (
                                type(error).__name__
                            ),
                            "error_message": str(error),
                        }
                    )

                    logger.error(
                        "Station %s failed: %s",
                        station_reference,
                        error,
                    )

        # Measure the complete multi-station batch duration.
        duration_seconds = round(
            time.perf_counter() - start_time,
            2,
        )

        # Sum observations from all successful stations.
        total_readings = sum(
            result["reading_count"]
            for result in successful_stations
        )

        # Return a batch-level summary.
        return {
            "batch_run_id": batch_run_id,
            "batch_start_timestamp": (
                batch_start_timestamp
            ),
            "stations_attempted": (
                len(station_references)
            ),
            "stations_succeeded": (
                len(successful_stations)
            ),
            "stations_failed": (
                len(failed_stations)
            ),
            "total_readings": total_readings,
            "duration_seconds": duration_seconds,
            "successful_stations": (
                successful_stations
            ),
            "failed_stations": failed_stations,
        }