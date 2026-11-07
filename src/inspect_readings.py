import json
import logging

from api.client import FloodMonitoringAPIClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def main():

    station_reference = "1029TH"

    client = FloodMonitoringAPIClient()

    print(
        f"\nRequesting today's readings "
        f"for station {station_reference}...\n"
    )

    data = client.get_station_readings(
        station_reference=station_reference,
        today=True,
    )

    print("Request successful!")

    print(
        f"Top-level JSON keys: "
        f"{list(data.keys())}"
    )

    readings = data.get("items", [])

    print(
        f"Number of readings returned: "
        f"{len(readings)}"
    )

    if readings:

        print("\nFirst reading:")

        print(
            json.dumps(
                readings[0],
                indent=2,
            )
        )

        print("\nLast reading:")

        print(
            json.dumps(
                readings[-1],
                indent=2,
            )
        )

    else:
        print("No readings returned.")


if __name__ == "__main__":
    main()