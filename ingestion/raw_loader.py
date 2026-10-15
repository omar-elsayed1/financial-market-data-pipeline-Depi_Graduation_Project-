"""Raw payload loading helpers."""

from __future__ import annotations

import logging
from typing import Any

LOGGER = logging.getLogger(__name__)


def load_raw_records(symbol: str, records: list[dict[str, Any]]) -> int:
    """Load raw API records into the raw schema idempotently.

    Args:
        symbol: Market symbol.
        records: Raw records from provider.

    Returns:
        Number of records staged for insertion.
    """
    LOGGER.info("TODO: load %s raw records for symbol=%s with conflict-safe strategy", len(records), symbol)
    return len(records)
