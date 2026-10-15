-- TODO: Execute this schema before running the pipeline.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.market_data_responses (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    trade_date DATE NOT NULL,
    payload JSONB NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_raw_market_symbol_trade_date
    ON raw.market_data_responses (symbol, trade_date);

CREATE TABLE IF NOT EXISTS staging.market_data_clean (
    symbol TEXT NOT NULL,
    trade_date DATE NOT NULL,
    open_price NUMERIC,
    high_price NUMERIC,
    low_price NUMERIC,
    close_price NUMERIC,
    volume BIGINT,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_staging_market_symbol_trade_date
    ON staging.market_data_clean (symbol, trade_date);

CREATE TABLE IF NOT EXISTS analytics.market_data_metrics (
    symbol TEXT NOT NULL,
    trade_date DATE NOT NULL,
    daily_return NUMERIC,
    ma_7 NUMERIC,
    ma_30 NUMERIC,
    rolling_volatility_30 NUMERIC,
    rolling_min_30 NUMERIC,
    rolling_max_30 NUMERIC,
    volume_avg_30 NUMERIC,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_analytics_market_symbol_trade_date
    ON analytics.market_data_metrics (symbol, trade_date);
