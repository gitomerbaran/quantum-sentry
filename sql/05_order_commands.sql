-- v2 logger (rust/logger) bu tabloya yazar. Logger tabloyu oluşturmaz; bir kez bu DDL'i çalıştırın.
CREATE TABLE IF NOT EXISTS order_commands
(
    ts_ingest DateTime64(3, 'UTC'),
    client_order_id String,
    symbol String,
    side String,
    qty Float64,
    price Nullable(Float64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts_ingest)
ORDER BY (symbol, ts_ingest, client_order_id);
