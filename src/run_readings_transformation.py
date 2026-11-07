import logging

from transformation.readings import (
    ReadingTransformer,
)


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | %(levelname)s | "
        "%(name)s | %(message)s"
    ),
)


def main():

    input_file = (
        "data/raw/readings/"
        "start_date=2026-09-09/"
        "station=1029TH/"
        "readings_2026-09-09_2026-09-16_"
        "2d56d5c5-f35f-417e-8a66-a7456a1d44af.json"
    )

    transformer = (
        ReadingTransformer()
    )

    readings = (
        transformer.transform_file(
            input_file=input_file,
        )
    )

    print(
        f"\nTransformed readings: "
        f"{len(readings)}"
    )

    print(
        "\nFirst transformed reading:"
    )

    print(
        readings[0]
    )

    print(
        "\nLast transformed reading:"
    )

    print(
        readings[-1]
    )


if __name__ == "__main__":
    main()