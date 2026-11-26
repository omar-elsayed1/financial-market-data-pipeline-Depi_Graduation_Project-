"""First end-to-end run for ONE ticker: fetch -> save raw -> clean -> upsert.

Run from the project folder (Docker Postgres must be running):
    python scripts/run_one_ticker.py AAPL

Run it twice. The first run loads the full history. The second run only asks
the API for the newest days (latest stored date minus a few days of overlap)
and creates no duplicates. That is incremental loading.

TEMPORARY: quick_clean() below is a minimal stand-in so the whole path can be
proven today. When Transformation merges parse_and_clean() and add_metrics(),
replace quick_clean() with those functions.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # so "db" and "ingestion" import

from db.loaders import get_last_loaded_date, save_raw, upsert_prices  # noqa: E402
from ingestion.extract import LOOKBACK_DAYS  # noqa: E402
from ingestion.provider import fetch_symbol  # noqa: E402

LOGGER = logging.getLogger("run_one_ticker")

PRICE_COLUMNS = ["open", "high", "low", "close", "volume"]


def quick_clean(symbol: str, values: list[dict[str, Any]]) -> pd.DataFrame:
    """Minimal cleaning: text to numbers, UTC timestamps, drop unusable rows.

    Args:
        symbol: Market symbol.
        values: The ``values`` list from the API response (all text).

    Returns:
        DataFrame in the shared contract format (no metrics yet).
    """
    df = pd.DataFrame(values)
    df["trade_date"] = pd.to_datetime(df["datetime"]).dt.normalize()
    df["ts_utc"] = pd.to_datetime(df["datetime"], utc=True)
    for col in PRICE_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    before = len(df)
    df = df.dropna(subset=["close"]).drop_duplicates(subset="trade_date", keep="first")
    valid = (df["close"] > 0) & (df["high"] >= df["low"]) & (df["volume"].isna() | (df["volume"] >= 0))
    df = df[valid]
    if len(df) < before:
        LOGGER.warning("Dropped %s unusable rows for %s", before - len(df), symbol)

    df["volume"] = df["volume"].astype("Int64")
    df.insert(0, "symbol", symbol)
    return (
        df[["symbol", "trade_date", "ts_utc", *PRICE_COLUMNS]]
        .sort_values("trade_date")
        .reset_index(drop=True)
    )


def run_ticker(
    symbol: str,
    fetch: Callable[..., dict[str, Any]] = fetch_symbol,
) -> dict[str, Any]:
    """Run the pipeline once for one symbol.

    Args:
        symbol: Market symbol, e.g. "AAPL".
        fetch: Function that fetches the symbol (replaceable in tests).

    Returns:
        Summary with the start date used, raw row id and row counts.
    """
    last_loaded = get_last_loaded_date(symbol)
    start_date = last_loaded - timedelta(days=LOOKBACK_DAYS) if last_loaded else None
    LOGGER.info("%s: last loaded date=%s, requesting from=%s", symbol, last_loaded, start_date or "full history")

    record = fetch(symbol, start_date)
    raw_id = save_raw(record)
    df = quick_clean(symbol, record["payload"]["values"])
    inserted, updated = upsert_prices(df, source_raw_id=raw_id)

    summary = {
        "symbol": symbol,
        "requested_from": str(start_date) if start_date else "full history",
        "rows_from_api": len(record["payload"]["values"]),
        "raw_id": raw_id,
        "inserted": inserted,
        "updated": updated,
        "latest_date": str(get_last_loaded_date(symbol)),
    }
    LOGGER.info("%s: %s", symbol, summary)
    return summary


def main() -> None:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Run the pipeline once for one ticker.")
    parser.add_argument("symbol", nargs="?", default="AAPL", help="ticker symbol (default AAPL)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    summary = run_ticker(args.symbol.upper())
    print("\nDone:")
    for key, value in summary.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
