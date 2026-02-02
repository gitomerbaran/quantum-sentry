-- ClickHouse schema for market-persister (HFT Data Pipeline)
-- Run with: clickhouse-client --user qs_user --password qs_pass -d quantum -q "$(cat schema.sql)"
-- Or execute statements one by one.

-- Drop old tables (required when migrating from depth_ticks / old trade_ticks)
DROP TABLE IF EXISTS trade_ticks;
DROP TABLE IF EXISTS depth_ticks;
DROP TABLE IF EXISTS bbo_ticks;

-- Table 1: Executed trades (Binance trade stream)
-- ts_exchange = event time from exchange; ts_ingest = our processing time (latency tracking)
-- trade_id = Binance "t" (idempotency)
CREATE TABLE trade_ticks
(
    ts_exchange DateTime64(3, 'UTC'),
    ts_ingest DateTime64(3, 'UTC'),
    trade_id UInt64,
    symbol String,
    price Float64,
    quantity Float64,
    is_buyer_maker Bool
)
ENGINE = MergeTree
PARTITION BY toDate(ts_exchange)
ORDER BY (symbol, ts_exchange, trade_id);

-- Table 2: Best bid/offer (BBO) - market depth snapshot
-- ts_exchange = event time from exchange; ts_ingest = our processing time
-- update_id = Binance "u" (orderBookUpdateId, idempotency)
CREATE TABLE bbo_ticks
(
    ts_exchange DateTime64(3, 'UTC'),
    ts_ingest DateTime64(3, 'UTC'),
    update_id UInt64,
    symbol String,
    bid_price Float64,
    bid_qty Float64,
    ask_price Float64,
    ask_qty Float64
)
ENGINE = MergeTree
PARTITION BY toDate(ts_exchange)
ORDER BY (symbol, ts_exchange, update_id);

-- ------------------------------------------------------------
-- depth_snapshots: L2 order book snapshots (100ms); multi-level arrays
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS depth_snapshots
(
    symbol       String,
    ts_exchange  DateTime64(3, 'UTC'),
    ts_ingest    DateTime64(3, 'UTC'),
    bids_price   Array(Float64),
    bids_qty     Array(Float64),
    asks_price   Array(Float64),
    asks_qty     Array(Float64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts_exchange)
ORDER BY (symbol, ts_exchange)
SETTINGS index_granularity = 8192;

-- ------------------------------------------------------------
-- liquidation_ticks: Binance Futures force order (liquidation) events
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS liquidation_ticks
(
    symbol          String,
    side            String,
    price           Float64,
    orig_qty        Float64,
    last_filled_qty Float64,
    ts_exchange     DateTime64(3, 'UTC'),
    ts_ingest       DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts_exchange)
ORDER BY (symbol, ts_exchange)
SETTINGS index_granularity = 8192;
