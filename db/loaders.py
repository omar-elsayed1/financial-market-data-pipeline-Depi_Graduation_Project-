"""Functions that read from and write to the pipeline database.

All other roles call these functions and never write SQL themselves.
Plain psycopg2 is used so the code works the same locally and inside Airflow.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Any

import pandas as pd
from psycopg2.extras import Json, execute_values

from db.connection import transaction

PRICE_COLUMNS = ["open", "high", "low", "close", "volume"]
METRIC_COLUMNS = [
    "daily_return", "ma_7", "ma_30", "volatility_30d",
    "avg_volume_30d", "min_close_30d", "max_close_30d",
]


def _to_python(value: Any) -> Any:
    """Convert pandas/numpy values (NaN, NaT, numpy numbers) to plain Python for SQL."""
    if value is None or (not isinstance(value, (str, bytes)) and pd.isna(value)):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if hasattr(value, "item"):  # numpy int64 / float64 -> int / float
        return value.item()
    return value


def payload_hash(payload: dict[str, Any]) -> str:
    """SHA-256 of the payload with sorted keys, so identical payloads give identical hashes."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def save_raw(
    record: dict[str, Any],
    provider: str = "twelve_data",
    endpoint: str = "time_series",
) -> int:
    """Store one API response in raw.api_responses, exactly as received.

    The same payload for the same symbol is stored only once. Calling this
    again with identical data returns the id of the existing row.

    Args:
        record: Dict from ``fetch_symbol``: symbol, request_params,
            http_status, fetched_at and payload.
        provider: Provider name to store.
        endpoint: Endpoint name to store.

    Returns:
        The id (primary key) of the raw row.
    """
    digest = payload_hash(record["payload"])
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.api_responses
                (provider, symbol, endpoint, request_params, http_status,
                 payload, payload_hash, fetched_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, payload_hash) DO NOTHING
            RETURNING id
            """,
            (
                provider, record["symbol"], endpoint, Json(record["request_params"]),
                record["http_status"], Json(record["payload"]), digest,
                record["fetched_at"],
            ),
        )
        row = cur.fetchone()
        if row is None:  # already stored: look up the existing row
            cur.execute(
                "SELECT id FROM raw.api_responses WHERE symbol = %s AND payload_hash = %s",
                (record["symbol"], digest),
            )
            row = cur.fetchone()
        return int(row[0])


def get_last_loaded_date(symbol: str) -> date | None:
    """Return the newest trade_date stored for a symbol (the watermark), or None.

    Args:
        symbol: Market symbol, e.g. "AAPL".
    """
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(f.trade_date)
            FROM analytics.fact_daily_price f
            JOIN analytics.dim_symbol d USING (symbol_id)
            WHERE d.symbol = %s
            """,
            (symbol,),
        )
        return cur.fetchone()[0]


def get_recent_prices(symbol: str, n: int = 30) -> pd.DataFrame:
    """Return the last n stored rows for a symbol, oldest first.

    The Transformation step needs this history to compute rolling metrics
    (moving averages, volatility) correctly for newly arrived rows.

    Args:
        symbol: Market symbol.
        n: How many of the most recent trading days to return.

    Returns:
        DataFrame with symbol, trade_date, ts_utc, open, high, low, close,
        volume (empty if nothing is stored yet).
    """
    columns = ["symbol", "trade_date", "ts_utc", *PRICE_COLUMNS]
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT d.symbol, f.trade_date, f.ts_utc,
                   f.open, f.high, f.low, f.close, f.volume
            FROM analytics.fact_daily_price f
            JOIN analytics.dim_symbol d USING (symbol_id)
            WHERE d.symbol = %s
            ORDER BY f.trade_date DESC
            LIMIT %s
            """,
            (symbol, n),
        )
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=columns)
    for col in PRICE_COLUMNS:
        df[col] = pd.to_numeric(df[col])  # Decimal -> float
    return df.sort_values("trade_date").reset_index(drop=True)


def upsert_prices(df: pd.DataFrame, source_raw_id: int | None = None) -> tuple[int, int]:
    """Insert new daily rows, or update rows that already exist.

    A row is identified by (symbol, trade_date). Running this twice with the
    same data changes nothing, which makes pipeline reruns safe. Everything
    happens in one transaction: if any row is invalid, nothing is saved.

    Args:
        df: Clean DataFrame with symbol, trade_date, ts_utc, the price
            columns, and optionally the metric columns.
        source_raw_id: Id of the raw row these prices came from (optional).

    Returns:
        (inserted, updated) row counts.
    """
    if df.empty:
        return 0, 0

    all_columns = ["trade_date", "ts_utc", *PRICE_COLUMNS, *METRIC_COLUMNS]
    with transaction() as conn, conn.cursor() as cur:
        symbols = sorted(df["symbol"].unique())
        execute_values(
            cur,
            "INSERT INTO analytics.dim_symbol (symbol) VALUES %s ON CONFLICT (symbol) DO NOTHING",
            [(s,) for s in symbols],
        )
        cur.execute(
            "SELECT symbol, symbol_id FROM analytics.dim_symbol WHERE symbol = ANY(%s)",
            (symbols,),
        )
        symbol_ids = dict(cur.fetchall())

        rows = []
        for record in df.to_dict("records"):
            values = [_to_python(record.get(col)) for col in all_columns]
            values[0] = pd.Timestamp(record["trade_date"]).date()  # trade_date as a plain date
            rows.append((symbol_ids[record["symbol"]], *values, source_raw_id))

        insert_columns = ["symbol_id", *all_columns, "source_raw_id"]
        update_set = ", ".join(
            f"{col} = EXCLUDED.{col}" for col in all_columns + ["source_raw_id"]
        )
        results = execute_values(
            cur,
            f"""
            INSERT INTO analytics.fact_daily_price ({", ".join(insert_columns)})
            VALUES %s
            ON CONFLICT (symbol_id, trade_date) DO UPDATE
            SET {update_set}, loaded_at = now()
            RETURNING (xmax = 0) AS inserted
            """,
            rows,
            page_size=1000,
            fetch=True,
        )
    inserted = sum(1 for (was_inserted,) in results if was_inserted)
    return inserted, len(results) - inserted


def log_run(
    dag_id: str,
    task_id: str,
    symbol: str | None,
    status: str,
    rows_read: int | None = None,
    rows_loaded: int | None = None,
    started_at: datetime | None = None,
    error_message: str | None = None,
) -> int:
    """Write one row to ops.pipeline_run_log and return its run_id.

    Args:
        dag_id: Airflow DAG id.
        task_id: Airflow task id.
        symbol: Symbol the task handled, if any.
        status: 'running', 'success' or 'failed'.
        rows_read: Rows read by the task.
        rows_loaded: Rows written by the task.
        started_at: When the task started (defaults to now).
        error_message: Error text when status is 'failed'.
    """
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.pipeline_run_log
                (dag_id, task_id, symbol, status, rows_read, rows_loaded,
                 started_at, finished_at, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, COALESCE(%s, now()),
                    CASE WHEN %s = 'running' THEN NULL ELSE now() END, %s)
            RETURNING run_id
            """,
            (dag_id, task_id, symbol, status, rows_read, rows_loaded,
             started_at, status, error_message),
        )
        return int(cur.fetchone()[0])


def save_rejected(
    symbol: str,
    trade_date: date | None,
    reason: str,
    row_data: dict[str, Any] | None = None,
) -> None:
    """Record a row that failed validation, with the reason, in ops.rejected_rows."""
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.rejected_rows (symbol, trade_date, reason, row_data)
            VALUES (%s, %s, %s, %s)
            """,
            (symbol, trade_date, reason, Json(row_data, dumps=lambda o: json.dumps(o, default=str))),
        )


def save_quality(
    check_name: str,
    status: str,
    metric_value: float | None = None,
    symbol: str | None = None,
    run_id: int | None = None,
) -> None:
    """Record one data-quality check result in ops.data_quality_results.

    Args:
        check_name: Name of the check, e.g. "duplicate_rows".
        status: 'pass', 'warn' or 'fail'.
        metric_value: The measured number (e.g. how many duplicates).
        symbol: Symbol the check applies to, if any.
        run_id: Related run in ops.pipeline_run_log, if any.
    """
    with transaction() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.data_quality_results
                (run_id, check_name, symbol, metric_value, status)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (run_id, check_name, symbol, metric_value, status),
        )
