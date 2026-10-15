"""Analytical metrics for transformed market data."""

from __future__ import annotations

import logging

import pandas as pd

LOGGER = logging.getLogger(__name__)


def compute_metrics(frame: pd.DataFrame) -> pd.DataFrame:
    """Compute required market metrics.

    Metrics include daily returns, 7/30-day moving averages, volume statistics,
    rolling volatility, and rolling min/max.

    Args:
        frame: Cleaned market data.

    Returns:
        DataFrame enriched with metric columns.
    """
    metrics_frame = frame.copy()
    LOGGER.info("TODO: calculate returns, moving averages, volume stats, volatility, and rolling min/max")
    return metrics_frame
