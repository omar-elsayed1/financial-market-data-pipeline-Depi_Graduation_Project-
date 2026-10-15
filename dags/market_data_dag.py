"""Airflow DAG definition for the Financial Market Data Pipeline."""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

LOGGER = logging.getLogger(__name__)


def _extract_task() -> None:
    """Run extraction task.

    TODO: orchestrate ingestion.extract incremental extraction by configured symbols.
    """
    LOGGER.info("TODO: extract task implementation")


def _load_raw_task() -> None:
    """Run raw loading task.

    TODO: persist raw provider responses into raw schema.
    """
    LOGGER.info("TODO: load_raw task implementation")


def _transform_task() -> None:
    """Run transformation task.

    TODO: run transform.clean and transform.metrics modules.
    """
    LOGGER.info("TODO: transform task implementation")


def _load_clean_task() -> None:
    """Run clean-data load task.

    TODO: upsert transformed datasets into staging/analytics schemas.
    """
    LOGGER.info("TODO: load_clean task implementation")


def _quality_check_task() -> None:
    """Run data quality checks task.

    TODO: run transform.quality and persist/report results.
    """
    LOGGER.info("TODO: quality_check task implementation")


def _on_failure_callback(context: dict) -> None:
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
        dag_id="market_data_pipeline",
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
