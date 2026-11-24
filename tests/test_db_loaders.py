"""Tests for db/loaders.py. They need the Docker Postgres to be running.

If the database is not reachable the whole file is skipped, so the normal
`python -m pytest` run still works on a machine without Docker.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
import pytest

from db import loaders
from db.connection import get_connection, transaction

SCHEMA_FILE = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"
TEST_SYMBOL = "ZZTEST"


def _database_available() -> bool:
    try:
        get_connection().close()
        return True
    except (psycopg2.OperationalError, KeyError):
        return False


pytestmark = pytest.mark.skipif(
    not _database_available(), reason="Postgres is not running (docker compose up -d)"
)


@pytest.fixture(autouse=True)
def clean_database():
    """Create the tables, then remove every row this test file creates."""
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(SCHEMA_FILE.read_text(encoding="utf-8"))
    yield
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM analytics.fact_daily_price WHERE symbol_id IN "
            "(SELECT symbol_id FROM analytics.dim_symbol WHERE symbol = %s)",
            (TEST_SYMBOL,),
        )
        cur.execute("DELETE FROM analytics.dim_symbol WHERE symbol = %s", (TEST_SYMBOL,))
        cur.execute("DELETE FROM raw.api_responses WHERE symbol = %s", (TEST_SYMBOL,))
        cur.execute("DELETE FROM ops.rejected_rows WHERE symbol = %s", (TEST_SYMBOL,))
        cur.execute("DELETE FROM ops.data_quality_results WHERE symbol = %s", (TEST_SYMBOL,))
        cur.execute("DELETE FROM ops.pipeline_run_log WHERE symbol = %s", (TEST_SYMBOL,))


def make_prices(days: int = 5, start: str = "2026-10-19", close: float = 100.0) -> pd.DataFrame:
    dates = pd.bdate_range(start, periods=days)
    return pd.DataFrame(
        {
            "symbol": TEST_SYMBOL,
            "trade_date": dates,
            "ts_utc": pd.to_datetime(dates, utc=True),
            "open": close, "high": close + 2, "low": close - 2, "close": close,
            "volume": 1_000_000,
        }
    )


def count_rows() -> int:
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM analytics.fact_daily_price f "
            "JOIN analytics.dim_symbol d USING (symbol_id) WHERE d.symbol = %s",
            (TEST_SYMBOL,),
        )
        return cur.fetchone()[0]


def test_upsert_inserts_rows_and_reports_counts():
    assert loaders.upsert_prices(make_prices(5)) == (5, 0)
    assert count_rows() == 5


def test_upsert_twice_does_not_create_duplicates():
    loaders.upsert_prices(make_prices(5))
    assert loaders.upsert_prices(make_prices(5)) == (0, 5)  # all updated, none inserted
    assert count_rows() == 5


def test_upsert_updates_changed_values():
    loaders.upsert_prices(make_prices(3, close=100.0))
    loaders.upsert_prices(make_prices(3, close=150.0))
    assert loaders.get_recent_prices(TEST_SYMBOL, 3)["close"].tolist() == [150.0] * 3


def test_upsert_mixes_new_and_existing_rows():
    loaders.upsert_prices(make_prices(3, start="2026-10-19"))
    inserted, updated = loaders.upsert_prices(make_prices(5, start="2026-10-19"))
    assert (inserted, updated) == (2, 3)
    assert count_rows() == 5


def test_upsert_handles_missing_metrics_and_nan():
    df = make_prices(2)
    df["ma_7"] = [float("nan"), 101.5]
    loaders.upsert_prices(df)
    assert count_rows() == 2


def test_invalid_row_rolls_back_the_whole_batch():
    df = make_prices(3)
    df.loc[2, "high"] = 1.0  # high below low breaks the CHECK constraint
    with pytest.raises(psycopg2.errors.CheckViolation):
        loaders.upsert_prices(df)
    assert count_rows() == 0  # nothing partially saved


def test_empty_dataframe_is_a_no_op():
    assert loaders.upsert_prices(make_prices(1).iloc[0:0]) == (0, 0)


def test_get_last_loaded_date_none_then_latest():
    assert loaders.get_last_loaded_date(TEST_SYMBOL) is None
    loaders.upsert_prices(make_prices(5, start="2026-10-19"))
    assert loaders.get_last_loaded_date(TEST_SYMBOL) == date(2026, 10, 23)


def test_get_recent_prices_returns_last_n_oldest_first():
    loaders.upsert_prices(make_prices(10, start="2026-10-05"))
    recent = loaders.get_recent_prices(TEST_SYMBOL, 4)
    assert len(recent) == 4
    assert recent["trade_date"].is_monotonic_increasing
    assert recent["trade_date"].iloc[-1] == date(2026, 10, 16)
    assert recent["close"].dtype.kind == "f"  # numbers, not Decimal objects


def test_get_recent_prices_for_unknown_symbol_is_empty():
    assert loaders.get_recent_prices("NOSUCHSYMBOL", 5).empty


def make_record(values_count: int = 2) -> dict:
    return {
        "symbol": TEST_SYMBOL,
        "request_params": {"symbol": TEST_SYMBOL, "interval": "1day"},
        "http_status": 200,
        "fetched_at": datetime(2026, 10, 24, 12, 0, tzinfo=timezone.utc).isoformat(),
        "payload": {"meta": {"symbol": TEST_SYMBOL}, "values": [{"close": "1"}] * values_count, "status": "ok"},
    }


def test_save_raw_stores_payload_unchanged():
    raw_id = loaders.save_raw(make_record())
    with transaction() as conn, conn.cursor() as cur:
        cur.execute("SELECT payload, http_status FROM raw.api_responses WHERE id = %s", (raw_id,))
        payload, status = cur.fetchone()
    assert payload == make_record()["payload"]
    assert status == 200


def test_save_raw_same_payload_twice_stores_one_row():
    first = loaders.save_raw(make_record())
    second = loaders.save_raw(make_record())
    assert first == second
    with transaction() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM raw.api_responses WHERE symbol = %s", (TEST_SYMBOL,))
        assert cur.fetchone()[0] == 1


def test_save_raw_different_payload_is_a_new_row():
    assert loaders.save_raw(make_record(2)) != loaders.save_raw(make_record(3))


def test_upsert_can_link_to_raw_row():
    raw_id = loaders.save_raw(make_record())
    loaders.upsert_prices(make_prices(2), source_raw_id=raw_id)
    with transaction() as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT source_raw_id FROM analytics.fact_daily_price f "
                    "JOIN analytics.dim_symbol d USING (symbol_id) WHERE d.symbol = %s", (TEST_SYMBOL,))
        assert cur.fetchall() == [(raw_id,)]


def test_ops_logging_functions_write_rows():
    run_id = loaders.log_run("dag", "task", TEST_SYMBOL, "success", rows_read=5, rows_loaded=5)
    loaders.save_rejected(TEST_SYMBOL, date(2026, 10, 20), "high below low", {"high": 1, "low": 2})
    loaders.save_quality("duplicate_rows", "pass", 0, TEST_SYMBOL, run_id)
    with transaction() as conn, conn.cursor() as cur:
        for table in ("ops.pipeline_run_log", "ops.rejected_rows", "ops.data_quality_results"):
            cur.execute(f"SELECT count(*) FROM {table} WHERE symbol = %s", (TEST_SYMBOL,))
            assert cur.fetchone()[0] == 1, table
