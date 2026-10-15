"""Incremental extraction utilities based on symbol-level watermarks."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

LOGGER = logging.getLogger(__name__)


def get_latest_stored_date(symbol: str) -> date | None:
    """Fetch the latest stored trade date for a symbol.

    Args:
        symbol: Market symbol.

    Returns:
        Most recent stored date or None when not found.
    """
    LOGGER.info("TODO: query latest stored date for symbol=%s", symbol)
    return None


def extract_incremental(symbol: str) -> list[dict[str, Any]]:
    """Extract incremental records newer than the latest watermark.

    Args:
        symbol: Market symbol.

    Returns:
        List of extracted records for raw loading.
    """
    watermark = get_latest_stored_date(symbol)
    LOGGER.info("TODO: extract incremental data for symbol=%s after watermark=%s", symbol, watermark)
    return []
