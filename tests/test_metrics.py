"""Basic tests for metrics transform module stubs."""

import pandas as pd

from transform.metrics import compute_metrics


def test_compute_metrics_returns_dataframe() -> None:
    """Metrics stub should return a dataframe copy."""
    frame = pd.DataFrame({"symbol": ["AAPL"], "close_price": [100.0]})
    metrics = compute_metrics(frame)
    assert isinstance(metrics, pd.DataFrame)
    assert len(metrics) == 1
