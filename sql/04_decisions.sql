-- v2 logger (rust/logger) bu tabloya yazar. Logger tabloyu oluşturmaz; bir kez bu DDL'i çalıştırın.
CREATE TABLE IF NOT EXISTS decisions
(
    ts_exchange DateTime64(3, 'UTC'),
    ts_ingest DateTime64(3, 'UTC'),
    symbol String,
    action String,
    confidence Float32,
    reason_codes Array(String),
    model_version String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts_exchange)
ORDER BY (symbol, ts_exchange, ts_ingest);
