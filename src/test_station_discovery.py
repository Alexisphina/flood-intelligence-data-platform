import logging

from ingestion.multi_station import MultiStationIngestor


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | %(levelname)s | "
        "%(name)s | %(message)s"
    ),
)


def main():

    ingestor = MultiStationIngestor(
        max_workers=2,
    )

    station_references = (
        ingestor.discover_stations()
    )

    print(
        f"\nStations discovered: "
        f"{len(station_references)}"
    )

    print(
        "First 3 station references: "
        f"{station_references[:3]}"
    )


if __name__ == "__main__":
    main()