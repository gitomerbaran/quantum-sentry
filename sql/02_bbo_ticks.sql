-- bbo_ticks: persister zaten oluşturuyor (ch.rs ensure_schema). Bu dosya sadece manuel kurulum için.
-- Persister ile aynı DDL (PARTITION BY toDate). Tek kaynak: rust/market-persister/schema.sql ve ch.rs.
CREATE TABLE IF NOT EXISTS bbo_ticks
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
