"""Daily market data pipeline.

For every ticker: fetch from the API -> save raw -> clean -> upsert (incremental).
Then a quality check records data freshness in ops.data_quality_results.

Schedule: weekdays at 23:30 UTC (after the US market has closed).
Retries: every task retries 3 times with growing waits.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml
from airflow.decorators import dag, task

LOGGER = logging.getLogger(__name__)

DAG_ID = "market_data_pipeline"
RATE_LIMIT_SECONDS = 8  # free tier allows 8 API calls per minute
FRESHNESS_LIMIT_DAYS = 5  # newest stored day older than this -> warning (weekends, holidays)
DEFAULT_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}


def load_symbols() -> list[str]:
    """Read the ticker list from config/symbols.yaml (falls back to a default list).

    Accepts ``symbols: [AAPL, ...]`` or a plain list, where each entry is a
    string or a mapping with a ``symbol`` key.
    """
    roots = [
        os.getenv("PROJECT_ROOT"),
        "/opt/airflow/project",  # where docker-compose mounts the repo
        str(Path(__file__).resolve().parents[1]),
    ]
    for root in filter(None, roots):
        path = Path(root) / "config" / "symbols.yaml"
        if path.exists():
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
            entries = data.get("symbols", []) if isinstance(data, dict) else data
            symbols = [e["symbol"] if isinstance(e, dict) else str(e) for e in entries or []]
            if symbols:
                return [s.upper() for s in symbols]
    LOGGER.warning("symbols.yaml not found or empty; using the default list")
    return DEFAULT_SYMBOLS


@dag(
    dag_id=DAG_ID,
    description="Fetch, clean and load daily market data for all tickers.",
    schedule="30 23 * * 1-5",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["market-data", "etl"],
)
def market_data_pipeline():
    """Define the tasks and their order."""

    @task
    def get_symbols() -> list[str]:
        """Return the tickers to process."""
        symbols = load_symbols()
        LOGGER.info("Processing %s symbols: %s", len(symbols), symbols)
        return symbols

    @task(max_active_tis_per_dag=1)  # one ticker at a time keeps us under the API rate limit
    def run_symbol(symbol: str) -> dict[str, Any]:
        """Run the pipeline for one ticker and log the outcome to ops.pipeline_run_log."""
        from db.loaders import log_run
        from scripts.run_one_ticker import run_ticker

        started = datetime.now(timezone.utc)
        try:
            summary = run_ticker(symbol)
        except Exception as error:  # log the failure, then let Airflow retry
            log_run(DAG_ID, "run_symbol", symbol, "failed",
                    started_at=started, error_message=str(error)[:500])
            raise
        log_run(DAG_ID, "run_symbol", symbol, "success",
                rows_read=summary["rows_from_api"],
                rows_loaded=summary["inserted"] + summary["updated"],
                started_at=started)
        time.sleep(RATE_LIMIT_SECONDS)
        return summary

    @task
    def quality_check(results: list[dict[str, Any]]) -> None:
        """Record a freshness check per ticker in ops.data_quality_results."""
        from datetime import date

        from db.loaders import get_last_loaded_date, save_quality

        for result in list(results):
            symbol = result["symbol"]
            latest = get_last_loaded_date(symbol)
            age_days = (date.today() - latest).days if latest else None
            status = "pass" if age_days is not None and age_days <= FRESHNESS_LIMIT_DAYS else "warn"
            save_quality("days_since_latest_row", status, age_days, symbol)
            save_quality("rows_loaded_this_run", "pass", result["inserted"] + result["updated"], symbol)
            LOGGER.info("%s: latest row %s (%s days old) -> %s", symbol, latest, age_days, status)

    results = run_symbol.expand(symbol=get_symbols())
    quality_check(results)


market_data_pipeline()
