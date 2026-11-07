import logging
from pathlib import Path
from typing import Any

import pandas as pd


logger = logging.getLogger(__name__)


class ParquetWriter:
    """
    Writes transformed records to Parquet files.
    """

    def __init__(
        self,
        processed_base_path: str = "data/processed",
    ):
        self.processed_base_path = Path(
            processed_base_path
        )

        self.processed_base_path.mkdir(
            parents=True,
            exist_ok=True,
        )

    def write(
        self,
        records: list[dict[str, Any]],
        dataset_name: str,
        output_filename: str,
    ) -> str:
        """
        Convert transformed records to a DataFrame
        and write them as Parquet.
        """

        if not records:
            raise ValueError(
                f"Cannot write empty dataset: "
                f"{dataset_name}"
            )

        output_directory = (
            self.processed_base_path
            / dataset_name
        )

        output_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_file = (
            output_directory
            / output_filename
        )

        dataframe = pd.DataFrame(
            records
        )

        logger.info(
            "Writing %s records to %s",
            len(dataframe),
            output_file,
        )

        for column in ("status", "date_opened"):
            if column in dataframe.columns:
                dataframe[column] = dataframe[column].astype("string")

        dataframe.to_parquet(
            output_file,
            engine="pyarrow",
            index=False,
        )

        logger.info(
            "Successfully wrote Parquet file %s",
            output_file,
        )

        return str(output_file)
