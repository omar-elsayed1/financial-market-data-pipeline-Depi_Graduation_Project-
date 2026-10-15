-- TODO: Use these starter analytics queries for dashboards and validation.

WITH ordered_prices AS (
    SELECT
        symbol,
        trade_date,
        close_price,
        AVG(close_price) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS running_avg_30
    FROM staging.market_data_clean
)
SELECT *
FROM ordered_prices;

WITH returns_ranked AS (
    SELECT
        symbol,
        trade_date,
        daily_return,
        RANK() OVER (PARTITION BY trade_date ORDER BY daily_return DESC) AS return_rank
    FROM analytics.market_data_metrics
)
SELECT *
FROM returns_ranked;

WITH month_close AS (
    SELECT
        symbol,
        DATE_TRUNC('month', trade_date)::DATE AS month_start,
        AVG(close_price) AS avg_month_close
    FROM staging.market_data_clean
    GROUP BY symbol, DATE_TRUNC('month', trade_date)
),
month_change AS (
    SELECT
        symbol,
        month_start,
        avg_month_close,
        avg_month_close
        - LAG(avg_month_close) OVER (PARTITION BY symbol ORDER BY month_start) AS month_over_month_change
    FROM month_close
)
SELECT *
FROM month_change;
