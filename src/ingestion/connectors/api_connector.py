import requests
from typing import List, Dict, Any

from src.ingestion.connectors.base import BaseConnector


class APIConnector(BaseConnector):
    """Fetches JSON records from a REST API endpoint."""

    def __init__(self, url: str, source_name: str = "api_source", timeout: int = 10):
        self.url = url
        self.source_name = source_name
        self.timeout = timeout

    def extract(self) -> List[Dict[str, Any]]:
        try:
            response = requests.get(self.url, timeout=self.timeout)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            raise TimeoutError(f"API call to {self.url} timed out after {self.timeout}s")
        except requests.exceptions.ConnectionError:
            raise ConnectionError(f"Could not connect to {self.url}")
        except requests.exceptions.HTTPError as e:
            raise RuntimeError(f"API returned an error status: {e}")

        data = response.json()

        if isinstance(data, dict):
            data = [data]

        return data