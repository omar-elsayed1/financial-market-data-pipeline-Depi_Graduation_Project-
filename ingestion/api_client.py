"""HTTP client utilities for market data providers."""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

LOGGER = logging.getLogger(__name__)


class APIClient:
    """Simple API client with retry, status checks, and JSON validation."""

    def __init__(self, base_url: str, api_key: str, timeout_seconds: int = 30) -> None:
        """Initialize the API client.

        Args:
            base_url: API provider base URL.
            api_key: Provider API key from environment variables.
            timeout_seconds: Request timeout.
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

    def get_json(self, endpoint: str, params: dict[str, Any], retries: int = 3) -> dict[str, Any]:
        """GET JSON with exponential backoff.

        Args:
            endpoint: API endpoint path.
            params: Query parameters.
            retries: Retry attempts.

        Returns:
            Parsed JSON object.

        Raises:
            requests.HTTPError: If all retries fail.
            ValueError: If payload is not a JSON object.
        """
        request_params = {**params, "apikey": self.api_key}
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        for attempt in range(1, retries + 1):
            try:
                response = self.session.get(url, params=request_params, timeout=self.timeout_seconds)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("Expected JSON object payload")
                return payload
            except (requests.RequestException, ValueError) as error:
                LOGGER.warning("API request failed on attempt %s/%s: %s", attempt, retries, error)
                if attempt == retries:
                    raise
                time.sleep(2 ** (attempt - 1))
        raise requests.HTTPError("Retry loop exhausted")
