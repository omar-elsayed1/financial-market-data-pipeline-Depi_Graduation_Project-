"""Optional MongoDB loader for original API JSON payloads.

Disabled by default; enable only when MongoDB extension is intentionally configured.
"""

from __future__ import annotations

import logging
from typing import Any

LOGGER = logging.getLogger(__name__)

MONGO_EXTENSION_ENABLED = False


def load_original_json(symbol: str, payload: dict[str, Any]) -> None:
    """Store original JSON in MongoDB when extension is enabled.

    Args:
        symbol: Market symbol.
        payload: Original API response payload.
    """
    if not MONGO_EXTENSION_ENABLED:
        LOGGER.info("MongoDB extension disabled; skipping JSON load for symbol=%s", symbol)
        return
    LOGGER.info("TODO: implement optional MongoDB loader for symbol=%s", symbol)
