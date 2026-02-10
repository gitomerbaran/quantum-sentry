-- Sim broker account snapshots table (portfolio state over time)
-- Created by: logger or manual setup
-- Usage: sim-broker writes account state via logger sink

CREATE TABLE IF NOT EXISTS sim_account_snapshots
(
    ts_exchange DateTime64(3),
    symbol String,
    cash_usdt Float64,
    position_side String,
    position_qty Float64,
    entry_price Float64,
    mark_price Float64,
    unrealized_pnl Float64,
    realized_pnl Float64,
    equity_usdt Float64,
    fees_paid Float64,
    trades_total UInt32,
    wins_total UInt32,
    losses_total UInt32
)
ENGINE = MergeTree()
PARTITION BY toDate(ts_exchange)
ORDER BY (symbol, ts_exchange)
TTL ts_exchange + INTERVAL 90 DAY;


