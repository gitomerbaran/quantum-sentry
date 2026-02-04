-- trade_ticks: persister zaten oluşturuyor (ch.rs ensure_schema). Bu dosya sadece manuel kurulum için.
-- Persister ile aynı DDL (PARTITION BY toDate). Tek kaynak: rust/market-persister/schema.sql ve ch.rs.
CREATE TABLE IF NOT EXISTS trade_ticks
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
