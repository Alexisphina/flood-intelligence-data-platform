import logging
from datetime import datetime, timedelta, timezone

from api.client import FloodMonitoringAPIClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def main():

    station_reference = "1029TH"

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=7)

    print("\n--- Seven-Day Reading Inspection ---\n")

    print(f"Station:    {station_reference}")
    print(f"Start date: {start_date}")
    print(f"End date:   {end_date}")

    client = FloodMonitoringAPIClient()

    data = client.get_station_readings(
        station_reference=station_reference,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )

    readings = data.get("items", [])

    print(f"\nReadings returned: {len(readings)}")

    meta = data.get("meta", {})

    print(f"API limit:  {meta.get('limit')}")
    print(f"API offset: {meta.get('offset')}")

    if readings:

        timestamps = [
            reading.get("dateTime")
            for reading in readings
            if reading.get("dateTime")
        ]

        measures = {
            reading.get("measure")
            for reading in readings
            if reading.get("measure")
        }

        print(f"Distinct measures: {len(measures)}")

        print(f"Earliest timestamp: {min(timestamps)}")
        print(f"Latest timestamp:   {max(timestamps)}")

        print("\nReadings per measure:")

        for measure in sorted(measures):

            count = sum(
                1
                for reading in readings
                if reading.get("measure") == measure
            )

            measure_id = measure.rsplit("/", 1)[-1]

            print(f"  {measure_id}: {count}")


if __name__ == "__main__":
    main()