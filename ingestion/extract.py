"""Incremental extraction based on symbol-level watermarks."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from ingestion.api_client import APIClient
from ingestion.provider import fetch_symbol

LOGGER = logging.getLogger(__name__)

LOOKBACK_DAYS = 5  # re-fetch a few days to catch late corrections; upserts make this safe

__all__ = ["LOOKBACK_DAYS", "extract_incremental", "fetch_symbol", "get_latest_stored_date"]


def get_latest_stored_date(symbol: str) -> date | None:
    """Fetch the latest stored trade date for a symbol (the watermark).

    Args:
        symbol: Market symbol.

    Returns:
        Most recent stored date or None when not found.
    """
    # TODO (Role C): replace with a call to db.loaders.get_last_loaded_date(symbol).
    LOGGER.info("TODO: query latest stored date for symbol=%s", symbol)
    return None


def extract_incremental(symbol: str, client: APIClient | None = None) -> list[dict[str, Any]]:
    """Extract records newer than the latest watermark (minus a small overlap).

    Args:
        symbol: Market symbol.
        client: Optional client (used by tests).

    Returns:
        List of data rows (newest first, values still as text) for raw loading.
    """
    watermark = get_latest_stored_date(symbol)
    start_date = watermark - timedelta(days=LOOKBACK_DAYS) if watermark else None
    LOGGER.info("Extracting symbol=%s watermark=%s start_date=%s", symbol, watermark, start_date)
    result = fetch_symbol(symbol, start_date, client)
    rows = result["payload"]["values"]
    LOGGER.info("Extracted %s rows for symbol=%s", len(rows), symbol)
    return rows
