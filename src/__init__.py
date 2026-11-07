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
            "label": station.get("label"),
            "river_name": station.get("riverName"),
            "catchment_name": station.get(
                "catchmentName"
            ),
            "town": station.get("town"),
            "latitude": station.get("lat"),
            "longitude": station.get("long"),
            "easting": station.get("easting"),
            "northing": station.get("northing"),
            "status": station.get("status"),
            "date_opened": station.get(
                "dateOpened"
            ),
            "rloi_id": station.get("RLOIid"),
            "wiski_id": station.get("wiskiID"),
            "notation": station.get("notation"),
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
            "parameter": measure.get(
                "parameter"
            ),
            "parameter_name": measure.get(
                "parameterName"
            ),
            "qualifier": measure.get(
                "qualifier"
            ),
            "unit_name": measure.get(
                "unitName"
            ),
            "period_seconds": measure.get(
                "period"
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

        for station in stations:

            if not isinstance(station, dict):
                raise ValueError(
                    "Invalid station record."
                )

            transformed_station = (
                self.transform_station(station)
            )

            transformed_stations.append(
                transformed_station
            )

            station_reference = (
                transformed_station[
                    "station_reference"
                ]
            )

            measures = station.get(
                "measures",
                [],
            )

            if not isinstance(measures, list):
                raise ValueError(
                    "Station measures must be a list."
                )

            for measure in measures:

                if not isinstance(measure, dict):
                    raise ValueError(
                        "Invalid measure record."
                    )

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
            "Transformed %s stations and %s measures",
            len(transformed_stations),
            len(transformed_measures),
        )

        return (
            transformed_stations,
            transformed_measures,
        )