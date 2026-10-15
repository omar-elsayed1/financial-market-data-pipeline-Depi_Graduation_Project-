# Architecture

```mermaid
flowchart LR
    S[Scheduler: Airflow DAG] --> E[Extract: REST API by symbol + watermark]
    E --> R[Load Raw: raw.market_data_responses]
    R --> T[Transform: clean + metrics with Pandas]
    T --> C[Load Clean: staging + analytics upserts]
    C --> Q[Quality Checks + Report]
```

## Notes
- Provider is configurable (default Alpha Vantage).
- Raw and transformed layers are stored separately.
- Pipeline is designed to be idempotent via unique keys and upserts.
