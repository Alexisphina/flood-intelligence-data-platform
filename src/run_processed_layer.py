import glob
import logging
import pandas as pd

from transformation.metadata import MetadataTransformer
from transformation.parquet_writer import ParquetWriter
from transformation.readings import ReadingTransformer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)


def deduplicate_readings(
    records: list[dict],
) -> list[dict]:
    """
    Keep the most recently ingested record for each
    measure and reading timestamp.
    """
    if not records:
        return []

    dataframe = pd.DataFrame(records)

    business_key = [
        "measure_id",
        "reading_timestamp",
    ]

    required_columns = business_key + [
        "station_reference",
        "value",
        "ingestion_timestamp",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            "Cannot deduplicate readings. "
            f"Missing columns: {missing_columns}"
        )

    # Validate that each measure belongs to one station.
    station_counts = (
        dataframe.groupby("measure_id")[
            "station_reference"
        ]
        .nunique()
    )

    inconsistent_measures = station_counts[
        station_counts > 1
    ]

    if not inconsistent_measures.empty:
        raise ValueError(
            "Some measure IDs map to multiple stations: "
            f"{inconsistent_measures.index.tolist()}"
        )

    # Detect observations whose values differ between runs.
    conflicting_values = (
        dataframe.groupby(business_key)["value"]
        .nunique(dropna=False)
        .gt(1)
        .sum()
    )

    if conflicting_values:
        logger.warning(
            "%s reading keys contain different values "
            "across extraction runs. Keeping the latest "
            "ingested record.",
            conflicting_values,
        )

    # Parse ingestion time so sorting is chronological.
    dataframe["_parsed_ingestion_timestamp"] = (
        pd.to_datetime(
            dataframe["ingestion_timestamp"],
            utc=True,
            errors="raise",
        )
    )

    dataframe = dataframe.sort_values(
        "_parsed_ingestion_timestamp"
    )

    original_count = len(dataframe)

    dataframe = dataframe.drop_duplicates(
        subset=business_key,
        keep="last",
    )

    removed_count = original_count - len(dataframe)

    dataframe = dataframe.drop(
        columns=["_parsed_ingestion_timestamp"]
    )

    logger.info(
        "Deduplicated readings from %s to %s records. "
        "Removed %s repeated observations.",
        original_count,
        len(dataframe),
        removed_count,
    )

    return dataframe.to_dict(orient="records")
def deduplicate_stations(
    records: list[dict],
) -> list[dict]:
    """Keep one record for each station reference."""
    if not records:
        return []

    dataframe = pd.DataFrame(records)

    if "station_reference" not in dataframe.columns:
        raise ValueError(
            "Cannot deduplicate stations. "
            "Missing column: station_reference"
        )

    references = (
        dataframe["station_reference"]
        .astype("string")
        .str.strip()
    )

    if references.isna().any() or references.eq("").any():
        raise ValueError(
            "Station reference cannot be null or blank."
        )

    dataframe["station_reference"] = references
    original_count = len(dataframe)

    dataframe = dataframe.drop_duplicates(
        subset=["station_reference"],
        keep="last",
    )

    logger.info(
        "Deduplicated stations from %s to %s records. "
        "Removed %s duplicate records.",
        original_count,
        len(dataframe),
        original_count - len(dataframe),
    )

    return dataframe.to_dict(orient="records")

def main():
    # 1. Find the latest timestamp-named metadata snapshot.
    station_files = sorted(
        glob.glob("data/raw/stations/*.json")
    )

    if not station_files:
        raise FileNotFoundError(
            "No station metadata snapshots found."
        )

    latest_station_file = station_files[-1]
    print(f"Station metadata source: {latest_station_file}")

    # 2. Transform station and measure metadata.
    metadata_transformer = MetadataTransformer()
    stations, measures = metadata_transformer.transform_file(
        input_file=latest_station_file
    )

    stations = deduplicate_stations(stations)

    # 3. Discover raw reading files once.
    reading_files = sorted(
        glob.glob(
            "data/raw/readings/**/*.json",
            recursive=True,
        )
    )

    print(f"Raw reading files discovered: {len(reading_files)}")

    if not reading_files:
        raise FileNotFoundError(
            "No raw reading files found."
        )

    # 4. Transform each raw file once.
    reading_transformer = ReadingTransformer()
    all_readings = []
    skipped_files = 0
    processed_files = 0
    empty_files = 0

    for reading_file in reading_files:
        try:
            transformed_readings = (
                reading_transformer.transform_file(
                    input_file=reading_file
                )
            )
        except ValueError as error:
            skipped_files += 1
            logger.warning(
                "Skipping incompatible raw file %s: %s",
                reading_file,
                error,
            )
            continue

        processed_files += 1

        if not transformed_readings:
            empty_files += 1

        all_readings.extend(transformed_readings)

        readings_before_deduplication = len(all_readings)

        all_readings = deduplicate_readings(
            all_readings
    )
    # Check before writing any datasets.
    for dataset_name, records in (
        ("stations", stations),
        ("measures", measures),
        ("readings", all_readings),
    ):
        if not records:
            raise ValueError(
                f"No records available for {dataset_name}; "
                "processed output was not written."
            )

    # 5. Write processed datasets.
    writer = ParquetWriter()

    stations_file = writer.write(
        records=stations,
        dataset_name="stations",
        output_filename="stations.parquet",
    )

    measures_file = writer.write(
        records=measures,
        dataset_name="measures",
        output_filename="measures.parquet",
    )

    readings_file = writer.write(
        records=all_readings,
        dataset_name="readings",
        output_filename="readings.parquet",
    )

    # 6. Report results.
    print("\n--- Processed Layer Summary ---")
    print(f"Stations: {len(stations)}")
    print(f"Measures: {len(measures)}")
    print(
        "Readings before deduplication: "
        f"{readings_before_deduplication}"
    )
    print(
        "Readings after deduplication: "
        f"{len(all_readings)}"
    )
    print(
        "Repeated observations removed: "
        f"{readings_before_deduplication - len(all_readings)}"
    )
    print(f"Raw files processed: {processed_files}")
    print(f"Empty reading files: {empty_files}")
    print(f"Incompatible files skipped: {skipped_files}")

    print(f"\nStations file: {stations_file}")
    print(f"Measures file: {measures_file}")
    print(f"Readings file: {readings_file}")


if __name__ == "__main__":
    main()
