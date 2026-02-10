-- Sim broker equity curve table (aggregated equity over time)
-- Created by: logger or manual setup
-- Usage: sim-broker writes equity snapshots via logger sink

CREATE TABLE IF NOT EXISTS sim_equity_curve
(
    ts_exchange DateTime64(3),
    equity_usdt Float64
)
ENGINE = MergeTree()
PARTITION BY toDate(ts_exchange)
ORDER BY ts_exchange
TTL ts_exchange + INTERVAL 90 DAY;


