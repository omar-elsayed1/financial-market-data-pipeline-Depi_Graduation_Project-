-- sql/schema.sql
-- Pipeline database layout. Safe to run more than once (everything is IF NOT EXISTS).
-- Layers:  raw       = API responses exactly as received (never edited)
--          analytics = cleaned daily prices plus metrics
--          ops       = run log, rejected rows, data-quality results

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS ops;

-- ---------------------------------------------------------------------------
-- RAW LAYER
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.api_responses (
    id              BIGSERIAL PRIMARY KEY,
    provider        TEXT        NOT NULL,                 -- e.g. 'twelve_data'
    symbol          TEXT        NOT NULL,
    endpoint        TEXT        NOT NULL,                 -- e.g. 'time_series'
    request_params  JSONB       NOT NULL,                 -- parameters sent (never the API key)
    http_status     INTEGER     NOT NULL,
    payload         JSONB       NOT NULL,                 -- the untouched JSON response
    payload_hash    TEXT        NOT NULL,                 -- SHA-256 of the canonical payload JSON
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- The same payload for the same symbol is stored only once, so reruns do not pile up copies.
    CONSTRAINT uq_raw_symbol_payload UNIQUE (symbol, payload_hash)
);

-- "Latest raw response for a symbol" lookups.
CREATE INDEX IF NOT EXISTS idx_raw_symbol_fetched
    ON raw.api_responses (symbol, fetched_at DESC);

-- ---------------------------------------------------------------------------
-- ANALYTICS LAYER
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_symbol (
    symbol_id   SERIAL      PRIMARY KEY,
    symbol      TEXT        NOT NULL UNIQUE,
    name        TEXT,
    exchange    TEXT,
    currency    TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS analytics.fact_daily_price (
    symbol_id        INTEGER       NOT NULL REFERENCES analytics.dim_symbol (symbol_id),
    trade_date       DATE          NOT NULL,              -- the trading day
    ts_utc           TIMESTAMPTZ   NOT NULL,              -- same moment normalised to UTC
    open             NUMERIC(18,6),
    high             NUMERIC(18,6),
    low              NUMERIC(18,6),
    close            NUMERIC(18,6) NOT NULL,
    volume           BIGINT,
    -- Metrics (NULL until enough history exists; never filled)
    daily_return     NUMERIC(12,8),
    ma_7             NUMERIC(18,6),
    ma_30            NUMERIC(18,6),
    volatility_30d   NUMERIC(12,8),
    avg_volume_30d   NUMERIC(20,2),
    min_close_30d    NUMERIC(18,6),
    max_close_30d    NUMERIC(18,6),
    -- Lineage
    source_raw_id    BIGINT REFERENCES raw.api_responses (id),
    loaded_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    -- One row per symbol per day: this is what makes upserts (reruns) safe.
    PRIMARY KEY (symbol_id, trade_date),
    -- Last line of defence. Transformation should already have removed such rows.
    CONSTRAINT ck_price_positive CHECK (close > 0),
    CONSTRAINT ck_high_low       CHECK (high IS NULL OR low IS NULL OR high >= low),
    CONSTRAINT ck_volume_nonneg  CHECK (volume IS NULL OR volume >= 0)
);

-- Date-range scans across all symbols (the primary key already serves per-symbol lookups).
CREATE INDEX IF NOT EXISTS idx_fact_trade_date
    ON analytics.fact_daily_price (trade_date);

-- ---------------------------------------------------------------------------
-- OPS LAYER
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.pipeline_run_log (
    run_id         BIGSERIAL PRIMARY KEY,
    dag_id         TEXT,
    task_id        TEXT,
    symbol         TEXT,
    status         TEXT        NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    rows_read      INTEGER,
    rows_loaded    INTEGER,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    error_message  TEXT
);

CREATE INDEX IF NOT EXISTS idx_run_log_started
    ON ops.pipeline_run_log (started_at DESC);

CREATE TABLE IF NOT EXISTS ops.rejected_rows (
    id           BIGSERIAL PRIMARY KEY,
    symbol       TEXT        NOT NULL,
    trade_date   DATE,
    reason       TEXT        NOT NULL,
    row_data     JSONB,
    detected_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.data_quality_results (
    id            BIGSERIAL PRIMARY KEY,
    run_id        BIGINT REFERENCES ops.pipeline_run_log (run_id),
    check_name    TEXT        NOT NULL,
    symbol        TEXT,
    metric_value  NUMERIC,
    status        TEXT        NOT NULL,                   -- e.g. 'pass', 'warn', 'fail'
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
