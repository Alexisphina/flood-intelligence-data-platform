import logging
from datetime import datetime, timedelta, timezone

from api.client import FloodMonitoringAPIClient
from ingestion.historical import HistoricalReadingIngestor


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def main():

    station_reference = "1029TH"

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=7)

    client = FloodMonitoringAPIClient()

    ingestor = HistoricalReadingIngestor(
        client=client,
    )

    result = ingestor.ingest_station(
        station_reference=station_reference,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )

    print("\n--- Historical Ingestion Complete ---")

    print(
        f"API requests: {result['api_request_count']}"
    )

    print(
        f"Duration: {result['duration_seconds']} seconds"
    )
    
    print(
        f"Pipeline run ID: {result['pipeline_run_id']}"
    )

    print(
        f"Ingestion timestamp: {result['ingestion_timestamp']}"
    )
    
    print(
        f"Station: {result['station_reference']}"
    )

    print(
        f"Readings: {result['reading_count']}"
    )

    print(
        f"Raw file: {result['output_file']}"
    )


if __name__ == "__main__":
    main()