"""Tests for scripts/run_one_ticker.py using a fake API and the real Postgres.

Skipped automatically when the database is not running.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import psycopg2
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import run_one_ticker as runner  # noqa: E402
from db.connection import get_connection, transaction  # noqa: E402

SYMBOL = "ZZRUN"
SCHEMA_FILE = ROOT / "sql" / "schema.sql"


def _database_available() -> bool:
    try:
        get_connection().close()
        return True
    except (psycopg2.OperationalError, KeyError):
        return False


needs_db = pytest.mark.skipif(not _database_available(), reason="Postgres is not running")


def fake_payload(first_day: str, days: int) -> dict:
    """A response shaped like Twelve Data's: newest row first, every value text."""
    dates = pd.bdate_range(first_day, periods=days)[::-1]
    values = [
        {"datetime": d.strftime("%Y-%m-%d"), "open": "100.0", "high": "102.0",
         "low": "98.0", "close": "101.0", "volume": "5000"}
        for d in dates
    ]
    return {"meta": {"symbol": SYMBOL}, "values": values, "status": "ok"}


def make_fetch(all_days: int, first_day: str = "2026-09-01"):
    """Fake fetch_symbol that honours start_date, like the real API does."""
    calls = []

    def fetch(symbol, start_date=None):
        calls.append(start_date)
        payload = fake_payload(first_day, all_days)
        if start_date is not None:
            payload["values"] = [v for v in payload["values"] if v["datetime"] >= str(start_date)]
        return {
            "symbol": symbol,
            "request_params": {"symbol": symbol, "start_date": str(start_date)},
            "http_status": 200,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }

    fetch.calls = calls
    return fetch


@pytest.fixture
def clean_database():
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_FILE.read_text(encoding="utf-8"))
    yield
    with transaction() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM analytics.fact_daily_price WHERE symbol_id IN "
                    "(SELECT symbol_id FROM analytics.dim_symbol WHERE symbol = %s)", (SYMBOL,))
        cur.execute("DELETE FROM analytics.dim_symbol WHERE symbol = %s", (SYMBOL,))
        cur.execute("DELETE FROM raw.api_responses WHERE symbol = %s", (SYMBOL,))


def stored_rows() -> int:
    with transaction() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM analytics.fact_daily_price f "
                    "JOIN analytics.dim_symbol d USING (symbol_id) WHERE d.symbol = %s", (SYMBOL,))
        return cur.fetchone()[0]


@needs_db
def test_first_run_loads_full_history(clean_database):
    fetch = make_fetch(all_days=30)
    summary = runner.run_ticker(SYMBOL, fetch=fetch)
    assert fetch.calls == [None]  # no watermark -> full history requested
    assert (summary["inserted"], summary["updated"]) == (30, 0)
    assert stored_rows() == 30


@needs_db
def test_second_run_is_incremental_and_creates_no_duplicates(clean_database):
    runner.run_ticker(SYMBOL, fetch=make_fetch(all_days=30))
    first_last_day = date(2026, 10, 12)  # 30 business days from 2026-09-01

    fetch = make_fetch(all_days=33)  # three new trading days appeared at the API
    summary = runner.run_ticker(SYMBOL, fetch=fetch)

    # asks only for the newest days: latest stored date minus the 5-day overlap
    assert fetch.calls == [first_last_day - timedelta(days=5)]
    assert summary["inserted"] == 3
    assert summary["updated"] > 0  # overlap days are updated, not duplicated
    assert stored_rows() == 33


@needs_db
def test_running_the_same_data_twice_changes_nothing(clean_database):
    runner.run_ticker(SYMBOL, fetch=make_fetch(all_days=30))
    summary = runner.run_ticker(SYMBOL, fetch=make_fetch(all_days=30))
    assert summary["inserted"] == 0
    assert stored_rows() == 30


@needs_db
def test_identical_raw_response_is_stored_once(clean_database):
    runner.run_ticker(SYMBOL, fetch=make_fetch(all_days=30))
    runner.run_ticker(SYMBOL, fetch=make_fetch(all_days=30))  # different request -> different payload
    with transaction() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM raw.api_responses WHERE symbol = %s", (SYMBOL,))
        assert cur.fetchone()[0] >= 1


def test_quick_clean_converts_types_and_drops_bad_rows():
    values = [
        {"datetime": "2026-10-23", "open": "10", "high": "12", "low": "9", "close": "11", "volume": "100"},
        {"datetime": "2026-10-23", "open": "10", "high": "12", "low": "9", "close": "11", "volume": "100"},  # duplicate
        {"datetime": "2026-10-22", "open": "10", "high": "8", "low": "9", "close": "11", "volume": "100"},   # high < low
        {"datetime": "2026-10-21", "open": "10", "high": "12", "low": "9", "close": "oops", "volume": "100"},  # bad close
        {"datetime": "2026-10-20", "open": "10", "high": "12", "low": "9", "close": "0", "volume": "100"},   # close 0
        {"datetime": "2026-10-19", "open": "10", "high": "12", "low": "9", "close": "11", "volume": "-5"},   # negative volume
    ]
    df = runner.quick_clean("AAPL", values)
    assert len(df) == 1
    assert df.loc[0, "close"] == 11.0
    assert str(df["ts_utc"].dt.tz) == "UTC"
    assert list(df.columns) == ["symbol", "trade_date", "ts_utc", "open", "high", "low", "close", "volume"]
