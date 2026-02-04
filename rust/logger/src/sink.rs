//! Sink abstraction: MockSink (tests) and ClickHouseSink (feature).

use async_trait::async_trait;
use common::LogEvent;
use std::sync::atomic::{AtomicU64, Ordering};

/// Sink for log batches. Cold path only; hot path must not block.
#[async_trait]
pub trait LogSink: Send + Sync {
    async fn write_batch(
        &self,
        batch: &[LogEvent],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// In-memory sink for tests (no ClickHouse).
#[derive(Default)]
pub struct MockSink {
    pub batches: std::sync::Mutex<Vec<Vec<LogEvent>>>,
}

impl MockSink {
    pub fn new() -> Self {
        Self {
            batches: std::sync::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl LogSink for MockSink {
    async fn write_batch(
        &self,
        batch: &[LogEvent],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.batches
            .lock()
            .map_err(|_| "lock poisoned")?
            .push(batch.to_vec());
        Ok(())
    }
}

/// Counters for the async logger.
#[derive(Default)]
pub struct LoggerCounters {
    pub events_received: AtomicU64,
    pub events_dropped: AtomicU64,
    pub batches_flushed: AtomicU64,
    pub flush_errors: AtomicU64,
}

impl LoggerCounters {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_received(&self) {
        self.events_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_dropped(&self) {
        self.events_dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_flushed(&self) {
        self.batches_flushed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_flush_errors(&self) {
        self.flush_errors.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(feature = "clickhouse")]
mod clickhouse_sink {
    use super::*;
    use crate::config::ClickHouseConfig;
    use common::Action;
    use std::sync::Arc;

    /// Row types for ClickHouse (align with sql/*.sql).
    #[derive(clickhouse::Row, serde::Serialize)]
    struct TradeTickRow {
        ts_exchange: i64,
        ts_ingest: i64,
        trade_id: u64,
        symbol: String,
        price: f64,
        quantity: f64,
        is_buyer_maker: bool,
    }

    #[derive(clickhouse::Row, serde::Serialize)]
    struct BboTickRow {
        ts_exchange: i64,
        ts_ingest: i64,
        update_id: u64,
        symbol: String,
        bid_price: f64,
        bid_qty: f64,
        ask_price: f64,
        ask_qty: f64,
    }

    #[derive(clickhouse::Row, serde::Serialize)]
    struct FeatureSnapshotRow {
        ts_exchange: i64,
        ts_ingest: i64,
        symbol: String,
        features: Vec<f32>,
        ready: u8,
        data_latency_ms: u32,
        spread_pct: f32,
        feature_version: u32,
    }

    #[derive(clickhouse::Row, serde::Serialize)]
    struct DecisionRow {
        ts_exchange: i64,
        ts_ingest: i64,
        symbol: String,
        action: String,
        confidence: f32,
        reason_codes: Vec<String>,
        model_version: String,
    }

    #[derive(clickhouse::Row, serde::Serialize)]
    struct OrderCommandRow {
        ts_ingest: i64,
        client_order_id: String,
        symbol: String,
        side: String,
        qty: f64,
        price: Option<f64>,
    }

    fn action_str(a: &Action) -> &'static str {
        match a {
            Action::Long => "Long",
            Action::Short => "Short",
            Action::NoTrade => "NoTrade",
        }
    }

    fn side_str(s: &common::OrderSide) -> &'static str {
        match s {
            common::OrderSide::Buy => "Buy",
            common::OrderSide::Sell => "Sell",
        }
    }

    pub struct ClickHouseSink {
        client: Arc<clickhouse::Client>,
        _config: ClickHouseConfig,
    }

    impl ClickHouseSink {
        pub fn new(
            config: &ClickHouseConfig,
        ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
            let client = clickhouse::Client::default()
                .with_url(&config.url)
                .with_database(&config.database)
                .with_user(&config.username)
                .with_password(&config.password);
            Ok(Self {
                client: Arc::new(client),
                _config: config.clone(),
            })
        }
    }

    #[async_trait]
    impl LogSink for ClickHouseSink {
        async fn write_batch(
            &self,
            batch: &[LogEvent],
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let trade_rows: Vec<TradeTickRow> = batch
                .iter()
                .filter_map(|e| {
                    if let LogEvent::TradeTick { ts_ingest, tick } = e {
                        Some(TradeTickRow {
                            ts_exchange: tick.ts_exchange,
                            ts_ingest: *ts_ingest,
                            trade_id: tick.exchange_id,
                            symbol: tick.symbol.clone(),
                            price: tick.price,
                            quantity: tick.qty,
                            is_buyer_maker: tick.is_buyer_maker,
                        })
                    } else {
                        None
                    }
                })
                .collect();
            if !trade_rows.is_empty() {
                let mut insert = self.client.insert::<TradeTickRow>("trade_ticks").await?;
                for r in &trade_rows {
                    insert.write(r).await?;
                }
                insert.end().await?;
            }

            let bbo_rows: Vec<BboTickRow> = batch
                .iter()
                .filter_map(|e| {
                    if let LogEvent::BboTick { ts_ingest, tick } = e {
                        Some(BboTickRow {
                            ts_exchange: tick.ts_exchange,
                            ts_ingest: *ts_ingest,
                            update_id: tick.update_id,
                            symbol: tick.symbol.clone(),
                            bid_price: tick.bid_price,
                            bid_qty: tick.bid_qty,
                            ask_price: tick.ask_price,
                            ask_qty: tick.ask_qty,
                        })
                    } else {
                        None
                    }
                })
                .collect();
            if !bbo_rows.is_empty() {
                let mut insert = self.client.insert::<BboTickRow>("bbo_ticks").await?;
                for r in &bbo_rows {
                    insert.write(r).await?;
                }
                insert.end().await?;
            }

            let snap_rows: Vec<FeatureSnapshotRow> = batch
                .iter()
                .filter_map(|e| {
                    if let LogEvent::FeatureSnapshot { ts_ingest, snap } = e {
                        Some(FeatureSnapshotRow {
                            ts_exchange: snap.ts_exchange,
                            ts_ingest: *ts_ingest,
                            symbol: snap.symbol.clone(),
                            features: snap.features.clone(),
                            ready: if snap.ready { 1 } else { 0 },
                            data_latency_ms: snap.data_latency_ms,
                            spread_pct: snap.spread_pct,
                            feature_version: snap.feature_version,
                        })
                    } else {
                        None
                    }
                })
                .collect();
            if !snap_rows.is_empty() {
                let mut insert = self
                    .client
                    .insert::<FeatureSnapshotRow>("feature_snapshots")
                    .await?;
                for r in &snap_rows {
                    insert.write(r).await?;
                }
                insert.end().await?;
            }

            let dec_rows: Vec<DecisionRow> = batch
                .iter()
                .filter_map(|e| {
                    if let LogEvent::Decision { ts_ingest, dec } = e {
                        Some(DecisionRow {
                            ts_exchange: dec.ts_exchange,
                            ts_ingest: *ts_ingest,
                            symbol: dec.symbol.clone(),
                            action: action_str(&dec.action).to_string(),
                            confidence: dec.confidence,
                            reason_codes: dec.reason_codes.clone(),
                            model_version: dec.model_version.clone(),
                        })
                    } else {
                        None
                    }
                })
                .collect();
            if !dec_rows.is_empty() {
                let mut insert = self.client.insert::<DecisionRow>("decisions").await?;
                for r in &dec_rows {
                    insert.write(r).await?;
                }
                insert.end().await?;
            }

            let cmd_rows: Vec<OrderCommandRow> = batch
                .iter()
                .filter_map(|e| {
                    if let LogEvent::OrderCommand { ts_ingest, cmd } = e {
                        Some(OrderCommandRow {
                            ts_ingest: *ts_ingest,
                            client_order_id: cmd.client_order_id.clone(),
                            symbol: cmd.symbol.clone(),
                            side: side_str(&cmd.side).to_string(),
                            qty: cmd.qty,
                            price: cmd.price,
                        })
                    } else {
                        None
                    }
                })
                .collect();
            if !cmd_rows.is_empty() {
                let mut insert = self
                    .client
                    .insert::<OrderCommandRow>("order_commands")
                    .await?;
                for r in &cmd_rows {
                    insert.write(r).await?;
                }
                insert.end().await?;
            }

            Ok(())
        }
    }
}

#[cfg(feature = "clickhouse")]
pub use clickhouse_sink::ClickHouseSink;
