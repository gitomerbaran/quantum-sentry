//! ClickHouse replay source: read bbo_ticks (and optionally trade_ticks) for symbol + time range.

use super::{ReplayEvent, ReplaySource};
use clickhouse::Row;
use common::BboTick;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct ClickHouseReplaySource {
    pub url: String,
    pub database: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub symbol: String,
    pub from_ts_ms: i64,
    pub to_ts_ms: i64,
    pub limit: Option<u64>,
    pub include_trades: bool,
}

#[derive(Row, Deserialize)]
struct BboRow {
    ts_exchange: i64,
    ts_ingest: i64,
    update_id: u64,
    symbol: String,
    bid_price: f64,
    bid_qty: f64,
    ask_price: f64,
    ask_qty: f64,
}

#[derive(Row, Deserialize)]
struct TradeRow {
    ts_exchange: i64,
    ts_ingest: i64,
    trade_id: u64,
    symbol: String,
    price: f64,
    quantity: f64,
    is_buyer_maker: bool,
}

#[async_trait::async_trait]
impl ReplaySource for ClickHouseReplaySource {
    async fn fetch_events(&self) -> anyhow::Result<Vec<ReplayEvent>> {
        let mut client = clickhouse::Client::default()
            .with_url(&self.url)
            .with_database(&self.database);
        if let Some(ref user) = self.username {
            client = client.with_user(user);
        }
        if let Some(ref pass) = self.password {
            client = client.with_password(pass);
        }

        let mut events = Vec::new();

        let limit_clause = self
            .limit
            .map(|n| format!("LIMIT {}", n))
            .unwrap_or_default();

        // Convert epoch ms to DateTime for ClickHouse comparison
        let from_dt = chrono::DateTime::from_timestamp_millis(self.from_ts_ms)
            .ok_or_else(|| anyhow::anyhow!("invalid from_ts_ms: {}", self.from_ts_ms))?;
        let to_dt = chrono::DateTime::from_timestamp_millis(self.to_ts_ms)
            .ok_or_else(|| anyhow::anyhow!("invalid to_ts_ms: {}", self.to_ts_ms))?;
        let from_str = from_dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        let to_str = to_dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string();

        let bbo_sql = format!(
            "SELECT ts_exchange, ts_ingest, update_id, symbol, bid_price, bid_qty, ask_price, ask_qty \
             FROM bbo_ticks \
             WHERE symbol = '{}' AND ts_exchange >= '{}' AND ts_exchange <= '{}' \
             ORDER BY ts_exchange, update_id \
             {}",
            self.symbol.replace('\'', "''"),
            from_str,
            to_str,
            limit_clause
        );

        let bbo_rows: Vec<BboRow> = client.query(&bbo_sql).fetch_all().await?;
        for row in bbo_rows {
            events.push(ReplayEvent::Bbo(BboTick {
                ts_exchange: row.ts_exchange,
                ts_ingest: row.ts_ingest,
                update_id: row.update_id,
                symbol: row.symbol,
                bid_price: row.bid_price,
                bid_qty: row.bid_qty,
                ask_price: row.ask_price,
                ask_qty: row.ask_qty,
            }));
        }

        if self.include_trades {
            let trade_sql = format!(
                "SELECT ts_exchange, ts_ingest, trade_id, symbol, price, quantity, is_buyer_maker \
                 FROM trade_ticks \
                 WHERE symbol = '{}' AND ts_exchange >= '{}' AND ts_exchange <= '{}' \
                 ORDER BY ts_exchange, trade_id \
                 {}",
                self.symbol.replace('\'', "''"),
                from_str,
                to_str,
                limit_clause
            );
            let trade_rows: Vec<TradeRow> = client.query(&trade_sql).fetch_all().await?;
            for row in trade_rows {
                events.push(ReplayEvent::Trade(common::TradeTick {
                    ts_exchange: row.ts_exchange,
                    ts_ingest: row.ts_ingest,
                    exchange_id: row.trade_id,
                    symbol: row.symbol,
                    price: row.price,
                    qty: row.quantity,
                    is_buyer_maker: row.is_buyer_maker,
                }));
            }
            events.sort();
        }

        Ok(events)
    }
}
