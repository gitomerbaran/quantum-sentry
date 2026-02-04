-- v2 logger (rust/logger) bu tabloya yazar. Logger tabloyu oluşturmaz; bir kez bu DDL'i çalıştırın.
-- Örnek: clickhouse-client -d quantum -q "$(cat sql/03_feature_snapshots.sql)"
CREATE TABLE IF NOT EXISTS feature_snapshots
(
    ts_exchange DateTime64(3, 'UTC'),
    ts_ingest DateTime64(3, 'UTC'),
    symbol String,
    features Array(Float32),
    ready UInt8,
    data_latency_ms UInt32,
    spread_pct Float32,
    feature_version UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts_exchange)
ORDER BY (symbol, ts_exchange, ts_ingest);
