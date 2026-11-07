import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)


class ReadingTransformer:
    """
    Transforms raw Environment Agency readings
    into normalized reading records.
    """

    def _extract_measure_id(
        self,
        measure_url: str,
    ) -> str:
        """
        Extract the measure identifier from its source URL.
        """

        if not measure_url:
            raise ValueError(
                "Reading is missing measure."
            )

        return measure_url.rstrip("/").split("/")[-1]

    def _validate_timestamp(
        self,
        timestamp: str,
    ) -> str:
        """
        Validate that the reading timestamp is valid ISO-8601.
        """

        if not timestamp:
            raise ValueError(
                "Reading is missing dateTime."
            )

        try:
            datetime.fromisoformat(
                timestamp.replace(
                    "Z",
                    "+00:00",
                )
            )

        except ValueError as error:
            raise ValueError(
                f"Invalid reading timestamp: "
                f"{timestamp}"
            ) from error

        return timestamp

    def transform_reading(
        self,
        reading: dict[str, Any],
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Transform one source reading.
        """

        measure_url = reading.get(
            "measure"
        )

        measure_id = (
            self._extract_measure_id(
                measure_url
            )
        )

        reading_timestamp = (
            self._validate_timestamp(
                reading.get("dateTime")
            )
        )

        value = reading.get("value")

        if value is None:
            raise ValueError(
                "Reading is missing value."
            )

        station_reference = metadata.get(
            "station_reference"
        )

        if not station_reference:
            raise ValueError(
                "Raw metadata is missing "
                "station_reference."
            )

        return {
            "measure_id": measure_id,
            "station_reference": (
                station_reference
            ),
            "reading_timestamp": (
                reading_timestamp
            ),
            "value": value,
            "pipeline_run_id": metadata.get(
                "pipeline_run_id"
            ),
            "batch_run_id": metadata.get(
                "batch_run_id"
            ),
            "ingestion_timestamp": metadata.get(
                "ingestion_timestamp"
            ),
        }

    def transform_file(
        self,
        input_file: str,
    ) -> list[dict[str, Any]]:
        """
        Transform all readings contained in one
        raw station ingestion file.
        """

        input_path = Path(
            input_file
        )

        logger.info(
            "Transforming raw readings file %s",
            input_path,
        )

        with input_path.open(
            "r",
            encoding="utf-8",
        ) as file:

            raw_data = json.load(
                file
            )

        if not isinstance(
            raw_data,
            dict,
        ):
            raise ValueError(
                "Raw readings file must contain "
                "a JSON object."
            )

        metadata = raw_data.get(
            "metadata"
        )

        readings = raw_data.get(
            "items"
        )

        if not isinstance(
            metadata,
            dict,
        ):
            raise ValueError(
                "Raw readings file is missing "
                "valid metadata."
            )

        if not isinstance(
            readings,
            list,
        ):
            raise ValueError(
                "Raw readings file is missing "
                "a valid items list."
            )

        transformed_readings = []

        rejected_reading_count = 0

        for reading in readings:

            if not isinstance(
                reading,
                dict,
            ):
                rejected_reading_count += 1

                logger.warning(
                    "Skipping invalid reading record."
                )

                continue

            try:
                transformed_reading = (
                    self.transform_reading(
                        reading=reading,
                        metadata=metadata,
                    )
                )

                transformed_readings.append(
                    transformed_reading
                )

            except ValueError as error:
                rejected_reading_count += 1

                logger.warning(
                    "Skipping invalid reading: %s",
                    error,
                )

        logger.info(
            "Transformed %s readings. "
            "Rejected readings: %s",
            len(transformed_readings),
            rejected_reading_count,
        )

        return transformed_readings