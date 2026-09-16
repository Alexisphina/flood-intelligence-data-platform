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