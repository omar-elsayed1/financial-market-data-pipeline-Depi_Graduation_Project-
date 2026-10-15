# Financial Market Data Pipeline

An automated data engineering pipeline that ingests historical and daily financial market data from a configurable REST provider (default Alpha Vantage), validates it, stores raw and transformed data separately in PostgreSQL, computes analytics with Pandas, and orchestrates daily runs using Apache Airflow.

## Architecture Overview
- **Ingestion**: Provider API client with retry/backoff and payload validation.
- **Storage**: Raw JSON payloads in `raw` schema and transformed datasets in `staging`/`analytics`.
- **Transformation**: Cleaning and metric generation via Pandas.
- **Load strategy**: Incremental extraction with symbol-level watermarks and idempotent upserts.
- **Orchestration**: Airflow DAG (`extract -> load_raw -> transform -> load_clean -> quality_check`).

See:
- `/docs/architecture.md`
- `/docs/erd.md`

## Setup
1. Use Python **3.11**.
2. Create and activate a virtual environment.
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Copy `.env.example` to `.env` and fill in real values.
5. Initialize PostgreSQL schema using `sql/schema.sql`.

## Run locally
- Run tests:
  ```bash
  pytest -q
  ```
- Run services with Docker Compose:
  ```bash
  docker compose up -d
  ```

## Trigger the Airflow DAG
1. Open Airflow UI at `http://localhost:8080`.
2. Enable DAG `market_data_pipeline`.
3. Trigger a run from the UI.

## Project structure
- `ingestion/`: API client, incremental extract, raw loading, optional MongoDB extension (disabled by default)
- `transform/`: cleaning, metrics, and data quality
- `sql/`: schema, upsert template, analytics SQL examples
- `dags/`: Airflow DAG definition
- `config/`: env settings and symbols list
- `docs/`: architecture, ERD, quality report template
- `tests/`: pytest starter tests
- `logs/`: runtime logs (`pipeline.log`)

## Team roles (placeholders)
- **Data Engineer:** TODO
- **Analytics Engineer:** TODO
- **ML/Quant Analyst:** TODO
- **DevOps Engineer:** TODO
- **Project Manager:** TODO
