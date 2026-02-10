-- Sim broker fills table (paper trading execution results)
-- Created by: logger or manual setup
-- Usage: sim-broker writes fills via logger sink

CREATE TABLE IF NOT EXISTS sim_fills
(
    ts_exchange DateTime64(3),
    symbol String,
    order_id String,
    action String,
    qty Float64,
    fill_price Float64,
    notional Float64,
    fee Float64,
    slippage_bps Float32,
    reason_code String
)
ENGINE = MergeTree()
PARTITION BY toDate(ts_exchange)
ORDER BY (symbol, ts_exchange, order_id)
TTL ts_exchange + INTERVAL 90 DAY;


