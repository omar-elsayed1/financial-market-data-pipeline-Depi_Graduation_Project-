"""Basic tests for clean transform module stubs."""

import pandas as pd

from transform.clean import clean_market_data


def test_clean_market_data_returns_dataframe() -> None:
    """Cleaning stub should return a dataframe copy."""
    frame = pd.DataFrame({"symbol": ["AAPL"], "close_price": [100.0]})
    cleaned = clean_market_data(frame)
    assert isinstance(cleaned, pd.DataFrame)
    assert len(cleaned) == 1
