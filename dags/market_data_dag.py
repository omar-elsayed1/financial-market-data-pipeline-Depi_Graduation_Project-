"""Airflow DAG definition for the Financial Market Data Pipeline."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from db.loaders import (
    get_last_loaded_date,
    get_recent_prices,
    log_run,
    save_quality,
    save_raw,
    upsert_prices,
)

LOGGER = logging.getLogger(__name__)

DAG_ID = "market_data_pipeline"
DEFAULT_SYMBOLS: list[str] = ["AAPL", "MSFT", "GOOGL"]


def _get_symbols() -> list[str]:
    """Return configured market symbols."""
    return DEFAULT_SYMBOLS


def _extract_task(ti: Any) -> list[dict[str, Any]]:
    """Run incremental extraction per symbol using last loaded date."""
    started_at = datetime.now(timezone.utc)
    run_id = log_run(DAG_ID, "extract", symbol=None, status="running", started_at=started_at)

    try:
        symbols = _get_symbols()
        extracted_records: list[dict[str, Any]] = []

        for symbol in symbols:
            last_date = get_last_loaded_date(symbol)
            LOGGER.info("Extracting %s since last_loaded_date=%s", symbol, last_date)

            # TODO: replace with ingestion extractor call returning dicts with keys:
            # symbol, request_params, http_status, fetched_at, payload
            records: list[dict[str, Any]] = []
            extracted_records.extend(records)

        ti.xcom_push(key="raw_records", value=extracted_records)
        log_run(
            DAG_ID,
            "extract",
            symbol=None,
            status="success",
            rows_read=len(extracted_records),
            rows_loaded=len(extracted_records),
            started_at=started_at,
        )
        return extracted_records
    except Exception as exc:
        log_run(
            DAG_ID,
            "extract",
            symbol=None,
            status="failed",
            started_at=started_at,
            error_message=str(exc),
        )
        LOGGER.exception("Extract task failed (run_id=%s)", run_id)
        raise


def _load_raw_task(ti: Any) -> dict[str, int]:
    """Persist raw provider records into raw.api_responses via save_raw."""
    started_at = datetime.now(timezone.utc)
    log_run(DAG_ID, "load_raw", symbol=None, status="running", started_at=started_at)

    try:
        records: list[dict[str, Any]] = ti.xcom_pull(task_ids="extract", key="raw_records") or []
        saved_ids: dict[str, int] = {}

        for idx, record in enumerate(records):
            raw_id = save_raw(record)
            key = f"{record['symbol']}_{idx}"
            saved_ids[key] = raw_id

        ti.xcom_push(key="raw_ids", value=saved_ids)
        log_run(
            DAG_ID,
            "load_raw",
            symbol=None,
            status="success",
            rows_read=len(records),
            rows_loaded=len(saved_ids),
            started_at=started_at,
        )
        return saved_ids
    except Exception as exc:
        log_run(
            DAG_ID,
            "load_raw",
            symbol=None,
            status="failed",
            started_at=started_at,
            error_message=str(exc),
        )
        raise


def _transform_task(ti: Any) -> dict[str, Any]:
    """Transform records using recent price history from get_recent_prices."""
    started_at = datetime.now(timezone.utc)
    log_run(DAG_ID, "transform", symbol=None, status="running", started_at=started_at)

    try:
        raw_records: list[dict[str, Any]] = ti.xcom_pull(task_ids="extract", key="raw_records") or []
        symbols = _get_symbols()

        history_by_symbol: dict[str, pd.DataFrame] = {
            symbol: get_recent_prices(symbol, n=30) for symbol in symbols
        }

        # TODO: wire in transform.clean and transform.metrics using raw_records and history_by_symbol
        _ = history_by_symbol
        clean_records: list[dict[str, Any]] = []

        ti.xcom_push(key="clean_records", value=clean_records)
        log_run(
            DAG_ID,
            "transform",
            symbol=None,
            status="success",
            rows_read=len(raw_records),
            rows_loaded=len(clean_records),
            started_at=started_at,
        )
        return {"clean_rows": len(clean_records)}
    except Exception as exc:
        log_run(
            DAG_ID,
            "transform",
            symbol=None,
            status="failed",
            started_at=started_at,
            error_message=str(exc),
        )
        raise


def _load_clean_task(ti: Any) -> tuple[int, int]:
    """Upsert transformed rows into analytics.fact_daily_price via upsert_prices."""
    started_at = datetime.now(timezone.utc)
    log_run(DAG_ID, "load_clean", symbol=None, status="running", started_at=started_at)

    try:
        clean_records: list[dict[str, Any]] = ti.xcom_pull(task_ids="transform", key="clean_records") or []
        raw_ids: dict[str, int] = ti.xcom_pull(task_ids="load_raw", key="raw_ids") or {}
        source_raw_id = next(iter(raw_ids.values()), None)

        df = pd.DataFrame(clean_records)
        inserted, updated = upsert_prices(df, source_raw_id=source_raw_id)

        log_run(
            DAG_ID,
            "load_clean",
            symbol=None,
            status="success",
            rows_read=len(clean_records),
            rows_loaded=inserted + updated,
            started_at=started_at,
        )
        return inserted, updated
    except Exception as exc:
        log_run(
            DAG_ID,
            "load_clean",
            symbol=None,
            status="failed",
            started_at=started_at,
            error_message=str(exc),
        )
        raise


def _quality_check_task(ti: Any) -> None:
    """Run data quality checks and persist results via save_quality."""
    started_at = datetime.now(timezone.utc)
    log_run(DAG_ID, "quality_check", symbol=None, status="running", started_at=started_at)

    try:
        clean_records: list[dict[str, Any]] = ti.xcom_pull(task_ids="transform", key="clean_records") or []

        # TODO: replace placeholder check with transform.quality results
        row_count = float(len(clean_records))
        save_quality(
            check_name="clean_row_count",
            status="pass" if row_count >= 0 else "fail",
            metric_value=row_count,
        )

        log_run(
            DAG_ID,
            "quality_check",
            symbol=None,
            status="success",
            rows_read=len(clean_records),
            rows_loaded=len(clean_records),
            started_at=started_at,
        )
    except Exception as exc:
        log_run(
            DAG_ID,
            "quality_check",
            symbol=None,
            status="failed",
            started_at=started_at,
            error_message=str(exc),
        )
        raise


def _on_failure_callback(context: dict[str, Any]) -> None:
    """Log Airflow task failure context."""
    LOGGER.error("Task failed: %s", context)


def create_dag() -> DAG:
    """Create and return the market data DAG."""
    default_args = {
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": _on_failure_callback,
    }

    dag = DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        description="Financial Market Data Pipeline DAG",
        schedule_interval="@daily",
        start_date=days_ago(1),
        catchup=False,
        tags=["finance", "etl"],
    )

    with dag:
        extract = PythonOperator(task_id="extract", python_callable=_extract_task)
        load_raw = PythonOperator(task_id="load_raw", python_callable=_load_raw_task)
        transform = PythonOperator(task_id="transform", python_callable=_transform_task)
        load_clean = PythonOperator(task_id="load_clean", python_callable=_load_clean_task)
        quality_check = PythonOperator(task_id="quality_check", python_callable=_quality_check_task)

        extract >> load_raw >> transform >> load_clean >> quality_check

    return dag


dag = create_dag()
