-- TODO: Parameterize and execute this statement from the loader layer.

INSERT INTO staging.market_data_clean (
    symbol,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume
) VALUES (
    %(symbol)s,
    %(trade_date)s,
    %(open_price)s,
    %(high_price)s,
    %(low_price)s,
    %(close_price)s,
    %(volume)s
)
ON CONFLICT (symbol, trade_date)
DO UPDATE SET
    open_price = EXCLUDED.open_price,
    high_price = EXCLUDED.high_price,
    low_price = EXCLUDED.low_price,
    close_price = EXCLUDED.close_price,
    volume = EXCLUDED.volume,
    loaded_at = NOW();
