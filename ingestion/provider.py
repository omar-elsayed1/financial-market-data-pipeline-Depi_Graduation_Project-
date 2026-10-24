"""Twelve Data provider adapter.

This is the ONLY module that knows Twelve Data's endpoint, parameters and
response shape. To switch provider, rewrite this file and keep the
`fetch_symbol` signature unchanged.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from ingestion.api_client import APIClient, ApiPayloadError

TIME_SERIES_ENDPOINT = "time_series"
INTERVAL = "1day"
MAX_OUTPUTSIZE = 5000  # rows per request; also overrides the API default of 30
REQUIRED_FIELDS = ("datetime", "open", "high", "low", "close", "volume")


def build_params(symbol: str, start_date: date | None = None) -> dict[str, Any]:
    """Build the query parameters for one symbol (without the API key).

    Args:
        symbol: Market symbol, e.g. "AAPL".
        start_date: Fetch from this date onward; None means full history.

    Returns:
        Query parameters for the time_series endpoint.
    """
    params: dict[str, Any] = {
        "symbol": symbol,
        "interval": INTERVAL,
        "outputsize": MAX_OUTPUTSIZE,
    }
    if start_date is not None:
        params["start_date"] = start_date.isoformat()
    return params


def validate_time_series(payload: dict[str, Any], symbol: str) -> list[dict[str, Any]]:
    """Check the structure of a time_series payload.

    Only the structure is checked here. Bad values (text that is not a number,
    negative prices, ...) are handled by the Transformation step.

    Args:
        payload: Parsed JSON from the API.
        symbol: Market symbol, used in error messages.

    Returns:
        The list of data rows.

    Raises:
        ApiPayloadError: When rows are missing, empty, or lack required fields.
    """
    values = payload.get("values")
    if not isinstance(values, list) or not values:
        raise ApiPayloadError(f"No data rows returned for {symbol}")
    for index, row in enumerate(values):
        if not isinstance(row, dict):
            raise ApiPayloadError(f"Row {index} for {symbol} is not an object")
        missing = [field for field in REQUIRED_FIELDS if field not in row]
        if missing:
            raise ApiPayloadError(f"Row {index} for {symbol} is missing fields: {missing}")
    return values


def _build_client() -> APIClient:
    """Create an APIClient from environment settings."""
    from config.settings import load_settings

    settings = load_settings()
    return APIClient(settings.api_base_url, settings.api_key)


def fetch_symbol(
    symbol: str,
    start_date: date | None = None,
    client: APIClient | None = None,
) -> dict[str, Any]:
    """Fetch one symbol and return the untouched response with request details.

    Args:
        symbol: Market symbol.
        start_date: Fetch from this date onward; None means full history.
        client: Optional client (used by tests); built from settings if omitted.

    Returns:
        Dict with symbol, request_params (no API key), http_status,
        fetched_at (UTC ISO timestamp) and payload (the raw JSON).
    """
    client = client or _build_client()
    params = build_params(symbol, start_date)
    payload = client.get_json(TIME_SERIES_ENDPOINT, params)
    validate_time_series(payload, symbol)
    return {
        "symbol": symbol,
        "request_params": params,
        "http_status": 200,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
