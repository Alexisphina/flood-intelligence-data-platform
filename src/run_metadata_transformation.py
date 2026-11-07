import glob
import logging

from transformation.metadata import (
    MetadataTransformer,
)


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | %(levelname)s | "
        "%(name)s | %(message)s"
    ),
)


def main():

    station_files = sorted(
        glob.glob(
            "data/raw/stations/*.json"
        )
    )

    if not station_files:
        raise FileNotFoundError(
            "No station metadata snapshots found."
        )

    latest_station_file = (
        station_files[-1]
    )

    print(
        f"Using station metadata file: "
        f"{latest_station_file}"
    )

    transformer = MetadataTransformer()

    stations, measures = (
        transformer.transform_file(
            input_file=latest_station_file,
        )
    )

    print(
        f"\nTransformed stations: "
        f"{len(stations)}"
    )

    print(
        f"Transformed measures: "
        f"{len(measures)}"
    )

    print(
        "\nFirst transformed station:"
    )

    print(
        stations[0]
    )

    print(
        "\nFirst transformed measure:"
    )

    print(
        measures[0]
    )


if __name__ == "__main__":
    main()