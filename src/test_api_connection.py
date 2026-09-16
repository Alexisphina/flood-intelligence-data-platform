import json
import logging

from api.client import FloodMonitoringAPIClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def main():
    print("\n--- Environment Agency Flood Monitoring API Test ---\n")

    client = FloodMonitoringAPIClient()

    print("Requesting station data...")

    data = client.get_stations()

    print("\nRequest successful!")
    print(f"Python object type: {type(data)}")
    print(f"Top-level JSON keys: {list(data.keys())}")

    items = data.get("items", [])

    print(f"Number of stations returned: {len(items)}")

    if items:
        print("\nFirst station:")
        print(json.dumps(items[0], indent=2))
    else:
        print("\nNo stations were returned.")


if __name__ == "__main__":
    main()