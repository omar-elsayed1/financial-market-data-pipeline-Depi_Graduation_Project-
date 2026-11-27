"""Tests for dags/market_data_dag.py.

The whole file is skipped where Airflow is not installed (for example your Windows
virtual environment). To run it inside Docker:
    docker compose exec airflow-scheduler airflow dags list-import-errors
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")

import psycopg2  # noqa: E402
from airflow.models import DagBag  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
DAG_ID = "market_data_pipeline"
TEST_SYMBOL = "ZZDAG"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(ROOT / "dags"), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    return bag.dags[DAG_ID]


def task_function(dag, task_id):
    task = dag.get_task(task_id)
    return getattr(task, "python_callable", None) or task.partial_kwargs["python_callable"]


def _database_available() -> bool:
    try:
        from db.connection import get_connection

        get_connection().close()
        return True
    except (psycopg2.OperationalError, KeyError):
        return False


needs_db = pytest.mark.skipif(not _database_available(), reason="Postgres is not running")


def test_dag_imports_without_errors(dag):
    assert dag.dag_id == DAG_ID


def test_dag_has_the_expected_tasks_in_order(dag):
    assert {t.task_id for t in dag.tasks} == {"get_symbols", "run_symbol", "quality_check"}
    assert dag.get_task("get_symbols").downstream_task_ids == {"run_symbol"}
    assert dag.get_task("run_symbol").downstream_task_ids == {"quality_check"}


def test_every_task_retries_three_times(dag):
    assert all(task.retries == 3 for task in dag.tasks)


def test_runs_on_weekdays_only_and_does_not_backfill(dag):
    assert dag.schedule_interval == "30 23 * * 1-5"
    assert dag.catchup is False


def test_only_one_ticker_runs_at_a_time(dag):
    assert dag.get_task("run_symbol").partial_kwargs["max_active_tis_per_dag"] == 1


def test_symbols_come_from_the_config_file(dag):
    symbols = task_function(dag, "get_symbols")()
    assert len(symbols) >= 1
    assert all(s == s.upper() for s in symbols)


@needs_db
def test_successful_run_is_logged(dag, monkeypatch):
    import scripts.run_one_ticker as runner
    from db.connection import transaction

    monkeypatch.setattr("time.sleep", lambda s: None)
    monkeypatch.setattr(runner, "run_ticker", lambda symbol: {
        "symbol": symbol, "rows_from_api": 10, "inserted": 4, "updated": 6})
    try:
        result = task_function(dag, "run_symbol")(TEST_SYMBOL)
        assert result["inserted"] == 4
        with transaction() as conn, conn.cursor() as cur:
            cur.execute("SELECT status, rows_read, rows_loaded FROM ops.pipeline_run_log WHERE symbol = %s",
                        (TEST_SYMBOL,))
            assert cur.fetchall() == [("success", 10, 10)]
    finally:
        _cleanup()


@needs_db
def test_failed_run_is_logged_and_still_raises(dag, monkeypatch):
    import scripts.run_one_ticker as runner
    from db.connection import transaction

    def boom(symbol):
        raise RuntimeError("API is down")

    monkeypatch.setattr(runner, "run_ticker", boom)
    try:
        with pytest.raises(RuntimeError):
            task_function(dag, "run_symbol")(TEST_SYMBOL)
        with transaction() as conn, conn.cursor() as cur:
            cur.execute("SELECT status, error_message FROM ops.pipeline_run_log WHERE symbol = %s",
                        (TEST_SYMBOL,))
            assert cur.fetchall() == [("failed", "API is down")]
    finally:
        _cleanup()


@needs_db
def test_quality_check_records_results(dag):
    from db.connection import transaction

    try:
        task_function(dag, "quality_check")([{"symbol": TEST_SYMBOL, "inserted": 1, "updated": 2}])
        with transaction() as conn, conn.cursor() as cur:
            cur.execute("SELECT check_name, status FROM ops.data_quality_results "
                        "WHERE symbol = %s ORDER BY check_name", (TEST_SYMBOL,))
            rows = cur.fetchall()
        assert [r[0] for r in rows] == ["days_since_latest_row", "rows_loaded_this_run"]
        assert rows[0][1] == "warn"  # nothing stored for this fake symbol, so it counts as stale
    finally:
        _cleanup()


def _cleanup() -> None:
    from db.connection import transaction

    with transaction() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM ops.data_quality_results WHERE symbol = %s", (TEST_SYMBOL,))
        cur.execute("DELETE FROM ops.pipeline_run_log WHERE symbol = %s", (TEST_SYMBOL,))
