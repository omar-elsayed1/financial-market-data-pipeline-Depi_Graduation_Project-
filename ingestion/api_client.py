"""HTTP client for market data providers (retry, backoff, validation)."""

from __future__ import annotations

import logging
import random
import time
from typing import Any

import requests

LOGGER = logging.getLogger(__name__)

MAX_RETRY_AFTER_SECONDS = 60


class ApiHttpError(requests.HTTPError):
    """The API answered with an error status, or the request kept failing.

    Attributes:
        status_code: HTTP (or API body) error code, None for network failures.
        retry_after: Value of the Retry-After header when present.
    """

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        retry_after: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


class ApiPayloadError(ValueError):
    """The API answered, but the JSON body is invalid or reports an error."""


def _is_retryable_status(status_code: int | None) -> bool:
    """Return True for errors worth retrying: rate limit (429) and server errors (5xx)."""
    return status_code is not None and (status_code == 429 or status_code >= 500)


class APIClient:
    """API client with retry/backoff, status checks, and JSON validation."""

    def __init__(self, base_url: str, api_key: str, timeout_seconds: int = 30) -> None:
        """Initialize the API client.

        Args:
            base_url: API base URL without a path, e.g. https://api.twelvedata.com
            api_key: Provider API key from environment variables.
            timeout_seconds: Request timeout.
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

    def _redact(self, text: str) -> str:
        """Remove the API key from any text before it is logged or raised."""
        return text.replace(self.api_key, "***") if self.api_key else text

    @staticmethod
    def _backoff_seconds(attempt: int, retry_after: str | None) -> float:
        """Seconds to wait before the next attempt (honors Retry-After, adds jitter)."""
        if retry_after:
            try:
                return min(float(retry_after), MAX_RETRY_AFTER_SECONDS)
            except ValueError:
                pass
        return 2 ** (attempt - 1) + random.uniform(0, 1)

    def _request_once(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        """Make one request and validate the response."""
        response = self.session.get(url, params=params, timeout=self.timeout_seconds)
        status = response.status_code
        if status >= 400:
            detail = ""
            try:
                detail = str(response.json().get("message", ""))[:200]
            except (ValueError, AttributeError):
                pass
            raise ApiHttpError(
                f"HTTP {status}: {detail}".strip(),
                status,
                response.headers.get("Retry-After"),
            )
        try:
            payload = response.json()
        except ValueError as error:
            raise ApiPayloadError("Response is not valid JSON") from error
        if not isinstance(payload, dict):
            raise ApiPayloadError("Expected JSON object payload")
        if payload.get("status") == "error":
            code = payload.get("code")
            detail = str(payload.get("message", ""))[:200]
            if isinstance(code, int) and _is_retryable_status(code):
                raise ApiHttpError(f"API error {code}: {detail}", code)
            raise ApiPayloadError(f"API reported an error (code {code}): {detail}")
        return payload

    def get_json(self, endpoint: str, params: dict[str, Any], retries: int = 3) -> dict[str, Any]:
        """GET JSON with exponential backoff.

        Retries only connection errors, timeouts, HTTP 429 and 5xx. Other 4xx
        errors mean the request itself is wrong, so they fail immediately.

        Args:
            endpoint: API endpoint path, e.g. "time_series".
            params: Query parameters (the API key is added here).
            retries: Maximum number of attempts.

        Returns:
            Parsed JSON object.

        Raises:
            ApiHttpError: Non-retryable HTTP error, or all attempts failed.
            ApiPayloadError: Invalid JSON, or the body reports an error.
        """
        request_params = {**params, "apikey": self.api_key}
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        status_code: int | None = None
        failure = ""
        for attempt in range(1, retries + 1):
            retry_after = None
            try:
                return self._request_once(url, request_params)
            except ApiHttpError as error:
                status_code = error.status_code
                failure = self._redact(str(error))
                if not _is_retryable_status(status_code):
                    LOGGER.error("API request rejected, not retrying: %s", failure)
                    raise
                retry_after = error.retry_after
            except (requests.ConnectionError, requests.Timeout) as error:
                status_code = None
                failure = f"{type(error).__name__}: {self._redact(str(error))}"

            LOGGER.warning("API request failed on attempt %s/%s: %s", attempt, retries, failure)
            if attempt < retries:
                time.sleep(self._backoff_seconds(attempt, retry_after))

        # "from None" hides the original exception, whose text may contain the key.
        raise ApiHttpError(
            f"Request failed after {retries} attempts: {failure}", status_code
        ) from None
