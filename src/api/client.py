import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://environment.data.gov.uk/flood-monitoring"


logger = logging.getLogger(__name__)


class FloodMonitoringAPIClient:

    def __init__(self, timeout: int = 30):
        self.timeout = timeout
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)

        session = requests.Session()

        session.mount("https://", adapter)

        return session

    def get_stations(self) -> dict[str, Any]:

        url = f"{BASE_URL}/id/stations.json"

        logger.info("Requesting monitoring stations from %s", url)

        response = self.session.get(
            url,
            timeout=self.timeout,
        )

        response.raise_for_status()

        return response.json()

    def get_station_readings(
        self,
        station_reference: str,
        start_date: str | None = None,
        end_date: str | None = None,
        reading_date: str | None = None,
        today: bool = False,
        limit: int = 10000,
    ) -> dict[str, Any]:

        url = (
            f"{BASE_URL}/id/stations/"
            f"{station_reference}/readings"
        )

        params = {
            "_limit": limit,
        }

        if today:
            params["today"] = ""

        if reading_date:
            params["date"] = reading_date

        if start_date:
            params["startdate"] = start_date

        if end_date:
            params["enddate"] = end_date

        logger.info(
            "Requesting readings for station %s",
            station_reference,
        )

        response = self.session.get(
            url,
            params=params,
            timeout=self.timeout,
        )

        response.raise_for_status()

        return response.json()