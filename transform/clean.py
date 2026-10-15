"""Data cleaning functions for market datasets."""

from __future__ import annotations

import logging

import pandas as pd

LOGGER = logging.getLogger(__name__)


def clean_market_data(frame: pd.DataFrame) -> pd.DataFrame:
    """Apply baseline cleaning rules.

    Rules: UTC timestamps, duplicate removal, missing value handling, and dtype normalization.

    Args:
        frame: Input market data.

    Returns:
        Cleaned DataFrame.
    """
    cleaned = frame.copy()
    LOGGER.info("TODO: enforce UTC timestamp conversion and dtype fixes")
    LOGGER.info("TODO: resolve duplicates and missing values")
    return cleaned
