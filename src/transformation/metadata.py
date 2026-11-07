import json
import logging
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)


class MetadataTransformer:
    """
    Transforms nested station metadata from the source API
    into normalized station and measure records.
    """

    def _extract_id(
        self,
        source_url: str,
    ) -> str:
        """
        Extract the final identifier from a source URL.
        """

        if not source_url:
            raise ValueError(
                "Source identifier is missing."
            )

        return source_url.rstrip("/").split("/")[-1]

    def _normalize_optional_json_text(
        self,
        value: Any,
    ) -> str | None:
        """Preserve structured metadata as JSON text."""
        if value is None:
            return None

        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False)

        return str(value)

    def _normalize_text(
        self,
        value: Any,
    ) -> str | None:
        """
        Normalize optional source text fields.

        The API may return some descriptive fields
        as either a string or a list of strings.
        """

        if value is None:
            return None

        if isinstance(value, list):
            return ", ".join(
                str(item)
                for item in value
                if item is not None
            )

        return str(value)

    def _normalize_number(
        self,
        value: Any,
    ) -> float | int | None:
        """
        Normalize numeric source fields.

        Some API records may contain a list where
        the target schema expects one numeric value.
        """

        if value is None:
            return None

        if isinstance(value, list):

            if not value:
                return None

            logger.warning(
                "Numeric field contained multiple values: %s. "
                "Using the first value.",
                value,
            )

            value = value[0]

        if isinstance(value, (int, float)):
            return value

        try:
            return float(value)

        except (TypeError, ValueError):
            logger.warning(
                "Unable to convert numeric value %s.",
                value,
            )

            return None

    def transform_station(
        self,
        station: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Transform one source station into the target
        station schema.
        """

        station_reference = station.get(
            "stationReference"
        )

        if not station_reference:
            raise ValueError(
                "Station is missing stationReference."
            )

        return {
            "station_reference": station_reference,
            "label": self._normalize_text(
                station.get("label")
            ),
            "river_name": self._normalize_text(
                station.get("riverName")
            ),
            "catchment_name": self._normalize_text(
                station.get("catchmentName")
            ),
            "town": self._normalize_text(
                station.get("town")
            ),
            "latitude": self._normalize_number(
                station.get("lat")
            ),
            "longitude": self._normalize_number(
                station.get("long")
            ),
            "easting": self._normalize_number(
                station.get("easting")
            ),
            "northing": self._normalize_number(
                station.get("northing")
            ),
            "status": self._normalize_optional_json_text(
                station.get("status")
            ),
            "date_opened": self._normalize_optional_json_text(
                station.get("dateOpened")
            ),
            "rloi_id": self._normalize_text(
                station.get("RLOIid")
            ),
            "wiski_id": self._normalize_text(
                station.get("wiskiID")
            ),
            "notation": self._normalize_text(
                station.get("notation")
            ),
            "source_station_url": station.get(
                "@id"
            ),
        }

    def transform_measure(
        self,
        measure: dict[str, Any],
        station_reference: str,
    ) -> dict[str, Any]:
        """
        Transform one nested measure into the target
        measure schema.
        """

        measure_url = measure.get("@id")

        if not measure_url:
            raise ValueError(
                "Measure is missing @id."
            )

        measure_id = self._extract_id(
            measure_url
        )

        return {
            "measure_id": measure_id,
            "station_reference": (
                station_reference
            ),
            "parameter": self._normalize_text(
                measure.get("parameter")
            ),
            "parameter_name": self._normalize_text(
                measure.get("parameterName")
            ),
            "qualifier": self._normalize_text(
                measure.get("qualifier")
            ),
            "unit_name": self._normalize_text(
                measure.get("unitName")
            ),
            "period_seconds": self._normalize_number(
                measure.get("period")
            ),
            "source_measure_url": measure_url,
        }

    def transform_file(
        self,
        input_file: str,
    ) -> tuple[
        list[dict[str, Any]],
        list[dict[str, Any]],
    ]:
        """
        Transform a station metadata snapshot into
        normalized station and measure records.
        """

        input_path = Path(input_file)

        logger.info(
            "Transforming station metadata file %s",
            input_path,
        )

        with input_path.open(
            "r",
            encoding="utf-8",
        ) as file:
            raw_data = json.load(file)

        if not isinstance(raw_data, dict):
            raise ValueError(
                "Station metadata file must contain "
                "a JSON object."
            )

        stations = raw_data.get("items")

        if not isinstance(stations, list):
            raise ValueError(
                "Station metadata file is missing "
                "a valid items list."
            )

        transformed_stations = []
        transformed_measures = []

        # Count source station records that cannot be
        # transformed because they lack a required key.
        rejected_station_count = 0

        for station in stations:

            # A station record must be a JSON object/dictionary.
            if not isinstance(station, dict):
                rejected_station_count += 1

                logger.warning(
                    "Skipping invalid station record: "
                    "expected a dictionary."
                )

                continue

            # stationReference is required because it will
            # become the station primary key.
            station_reference = station.get(
                "stationReference"
            )

            if not station_reference:
                rejected_station_count += 1

                logger.warning(
                    "Skipping station with missing "
                    "stationReference. Source ID: %s",
                    station.get("@id"),
                )

                continue

            # Transform the valid station.
            transformed_station = (
                self.transform_station(station)
            )

            transformed_stations.append(
                transformed_station
            )

            # Process the measures belonging to this station.
            measures = station.get(
                "measures",
                [],
            )

            if not isinstance(measures, list):
                logger.warning(
                    "Station %s has invalid measures. "
                    "Skipping measures.",
                    station_reference,
                )

                continue

            for measure in measures:

                if not isinstance(measure, dict):
                    logger.warning(
                        "Skipping invalid measure for "
                        "station %s.",
                        station_reference,
                    )

                    continue

                transformed_measure = (
                    self.transform_measure(
                        measure=measure,
                        station_reference=(
                            station_reference
                        ),
                    )
                )

                transformed_measures.append(
                    transformed_measure
                )

        logger.info(
            "Transformed %s stations and %s measures. "
            "Rejected stations: %s",
            len(transformed_stations),
            len(transformed_measures),
            rejected_station_count,
        )

        return (
            transformed_stations,
            transformed_measures,
        )
