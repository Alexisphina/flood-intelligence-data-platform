import logging
import os
from datetime import (
    datetime,
    timedelta,
    timezone,
)

from ingestion.multi_station import MultiStationIngestor


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | %(levelname)s | "
        "%(name)s | %(message)s"
    ),
)


def main():

    # Create the multi-station orchestrator.
    # Only two station ingestions may execute concurrently.
    multi_station_ingestor = MultiStationIngestor(
        max_workers=2,
    )

    # Dynamically retrieve station references.
    station_references = (
        multi_station_ingestor.discover_stations()
    )

    print(
        f"\nStations discovered: "
        f"{len(station_references)}"
    )

    # Process every station by default. MAX_STATIONS supports
    # smaller development and demonstration runs.
    selected_station_references = (
        station_references[:int(os.getenv("MAX_STATIONS"))]
        if os.getenv("MAX_STATIONS")
        else station_references
    )

    print(
        "Stations selected for run: "
        f"{len(selected_station_references)}"
    )

    # Calculate seven complete calendar days.
    end_date = datetime.now(
        timezone.utc
    ).date()

    start_date = (
        end_date - timedelta(days=7)
    )

    # Run the controlled multi-station ingestion.
    result = multi_station_ingestor.ingest_stations(
        station_references=selected_station_references,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )

    print(
        "\n--- Multi-Station Ingestion Summary ---"
    )

    print(
        f"Batch run ID: "
        f"{result['batch_run_id']}"
    )

    print(
        f"Batch start: "
        f"{result['batch_start_timestamp']}"
    )

    print(
        f"Stations attempted: "
        f"{result['stations_attempted']}"
    )

    print(
        f"Stations succeeded: "
        f"{result['stations_succeeded']}"
    )

    print(
        f"Stations failed: "
        f"{result['stations_failed']}"
    )

    print(
        f"Total readings: "
        f"{result['total_readings']}"
    )

    print(
        f"Duration: "
        f"{result['duration_seconds']} seconds"
    )

    # Display individual failures if any occur.
    if result["failed_stations"]:

        print("\nFailed stations:")

        for failure in result["failed_stations"]:

            print(
                f"  "
                f"{failure['station_reference']}: "
                f"{failure['error_type']} - "
                f"{failure['error_message']}"
            )


if __name__ == "__main__":
    main()