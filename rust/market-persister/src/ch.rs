use clickhouse::Client;
use tracing::{info, warn};

use crate::model::{
    BBOTickRow, DepthSnapshotRow, LiquidationTickRow, MarketContextRow, TradeTickRow,
};

#[derive(Clone)]
pub struct ClickHouseWriter {
    pub client: Client,
    pub database: String,
}

impl ClickHouseWriter {
    pub fn new_from_env() -> Self {
        let url =
            std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:8123".to_string());
        let database = std::env::var("CLICKHOUSE_DB")
            .or_else(|_| {
                std::env::var("CLICKHOUSE_DATABASE").inspect(|_| {
                    tracing::warn!("CLICKHOUSE_DATABASE is deprecated, use CLICKHOUSE_DB");
                })
            })
            .unwrap_or_else(|_| "quantum".to_string());
        let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "qs_user".to_string());
        let password =
            std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_else(|_| "qs_pass".to_string());

        let client = Client::default()
            .with_url(url)
            .with_database(database.clone())
            .with_user(user)
            .with_password(password);

        Self { client, database }
    }

    pub async fn ping(&self) -> Result<(), clickhouse::error::Error> {
        let _one: u8 = self.client.query("SELECT 1").fetch_one().await?;
        Ok(())
    }

    /// Ensure trade_ticks and bbo_ticks with ts_exchange/ts_ingest, unique IDs, partition by ts_exchange, order by (symbol, ts_exchange, id).
    pub async fn ensure_schema(&self) -> Result<(), clickhouse::error::Error> {
        let create_db = format!("CREATE DATABASE IF NOT EXISTS {}", self.database);
        if let Err(e) = self.client.query(&create_db).execute().await {
            warn!("CREATE DATABASE failed (continuing): {e}");
        }

        // Table 1: Executed trades (latency via ts_ingest, idempotency via trade_id)
        let trade_ddl = r#"
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
ORDER BY (symbol, ts_exchange, trade_id)
        "#;
        self.client.query(trade_ddl).execute().await?;
        info!("ClickHouse schema ensured: trade_ticks");

        // Table 2: Best bid/offer (BBO) - market sentiment; idempotency via update_id
        let bbo_ddl = r#"
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
ORDER BY (symbol, ts_exchange, update_id)
        "#;
        self.client.query(bbo_ddl).execute().await?;
        info!("ClickHouse schema ensured: bbo_ticks");

        // Table 3: Futures context (mark price, funding rate, open interest); Nullable for partial events.
        let context_ddl = r#"
CREATE TABLE IF NOT EXISTS market_context
(
    ts DateTime64(3, 'UTC'),
    ts_ingest DateTime64(3, 'UTC'),
    symbol String,
    mark_price Nullable(Float64),
    funding_rate Nullable(Float64),
    open_interest Nullable(Float64),
    next_funding_time Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (symbol, ts)
        "#;
        self.client.query(context_ddl).execute().await?;
        info!("ClickHouse schema ensured: market_context");

        // Table 4: L2 depth snapshots (100ms; top-of-book / multi-level arrays)
        let depth_snapshots_ddl = r#"
CREATE TABLE IF NOT EXISTS depth_snapshots
(
    symbol String,
    ts_exchange DateTime64(3, 'UTC'),
    ts_ingest DateTime64(3, 'UTC'),
    bids_price Array(Float64),
    bids_qty Array(Float64),
    asks_price Array(Float64),
    asks_qty Array(Float64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts_exchange)
ORDER BY (symbol, ts_exchange)
SETTINGS index_granularity = 8192
        "#;
        self.client.query(depth_snapshots_ddl).execute().await?;
        info!("ClickHouse schema ensured: depth_snapshots");

        // Table 5: Binance Futures force order (liquidation) events
        let liquidation_ddl = r#"
CREATE TABLE IF NOT EXISTS liquidation_ticks
(
    symbol String,
    side String,
    price Float64,
    orig_qty Float64,
    last_filled_qty Float64,
    ts_exchange DateTime64(3, 'UTC'),
    ts_ingest DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts_exchange)
ORDER BY (symbol, ts_exchange)
SETTINGS index_granularity = 8192
        "#;
        self.client.query(liquidation_ddl).execute().await?;
        info!("ClickHouse schema ensured: liquidation_ticks");

        Ok(())
    }

    pub async fn insert_context_batch(
        &self,
        rows: &[MarketContextRow],
    ) -> Result<(), clickhouse::error::Error> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut insert = self
            .client
            .insert::<MarketContextRow>("market_context")
            .await?;
        for r in rows {
            insert.write(r).await?;
        }
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_trade_batch(
        &self,
        rows: &[TradeTickRow],
    ) -> Result<(), clickhouse::error::Error> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut insert = self.client.insert::<TradeTickRow>("trade_ticks").await?;
        for r in rows {
            insert.write(r).await?;
        }
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_bbo_batch(
        &self,
        rows: &[BBOTickRow],
    ) -> Result<(), clickhouse::error::Error> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut insert = self.client.insert::<BBOTickRow>("bbo_ticks").await?;
        for r in rows {
            insert.write(r).await?;
        }
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_depth_snapshots_batch(
        &self,
        rows: &[DepthSnapshotRow],
    ) -> Result<(), clickhouse::error::Error> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut insert = self
            .client
            .insert::<DepthSnapshotRow>("depth_snapshots")
            .await?;
        for r in rows {
            insert.write(r).await?;
        }
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_liquidation_batch(
        &self,
        rows: &[LiquidationTickRow],
    ) -> Result<(), clickhouse::error::Error> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut insert = self
            .client
            .insert::<LiquidationTickRow>("liquidation_ticks")
            .await?;
        for r in rows {
            insert.write(r).await?;
        }
        insert.end().await?;
        Ok(())
    }

    /// Alias for insert_depth_snapshots_batch (used by depth writer loop).
    pub async fn insert_depth_batch(
        &self,
        rows: &[DepthSnapshotRow],
    ) -> Result<(), clickhouse::error::Error> {
        self.insert_depth_snapshots_batch(rows).await
    }

    /// Alias for insert_liquidation_batch (used by liq writer loop).
    pub async fn insert_liq_batch(
        &self,
        rows: &[LiquidationTickRow],
    ) -> Result<(), clickhouse::error::Error> {
        self.insert_liquidation_batch(rows).await
    }
}
