use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketEvent {
    pub stream: Option<String>,
    pub event_type: Option<String>,
    pub ts_ingest: Option<String>,
    pub data: Value,
}

/// Executed trade row for trade_ticks. ts_exchange = Binance event time; ts_ingest = our processing time (latency tracking).
#[derive(Debug, Clone, Serialize, clickhouse::Row)]
pub struct TradeTickRow {
    pub ts_exchange: i64,
    pub ts_ingest: i64,
    pub trade_id: u64,
    pub symbol: String,
    pub price: f64,
    pub quantity: f64,
    /// true = buyer was maker (seller was aggressor / sell-dump); false = seller was maker (buy-pump).
    pub is_buyer_maker: bool,
}

/// Normalized context event from NATS `market.context` (Futures: mark price, funding rate, open interest).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketContextEvent {
    pub ts_exchange: i64,
    pub ts_ingest: i64,
    pub symbol: String,
    pub mark_price: Option<f64>,
    pub funding_rate: Option<f64>,
    pub open_interest: Option<f64>,
    pub next_funding_time: Option<i64>,
    pub source: Option<String>,
}

/// ClickHouse row for market_context table (Nullable fields preserved).
#[derive(Debug, Clone, Serialize, clickhouse::Row)]
pub struct MarketContextRow {
    pub ts: i64,
    pub ts_ingest: i64,
    pub symbol: String,
    pub mark_price: Option<f64>,
    pub funding_rate: Option<f64>,
    pub open_interest: Option<f64>,
    pub next_funding_time: Option<i64>,
}

impl MarketContextEvent {
    pub fn to_row(&self) -> MarketContextRow {
        MarketContextRow {
            ts: self.ts_exchange,
            ts_ingest: self.ts_ingest,
            symbol: self.symbol.clone(),
            mark_price: self.mark_price,
            funding_rate: self.funding_rate,
            open_interest: self.open_interest,
            next_funding_time: self.next_funding_time,
        }
    }
}

/// Best bid/offer row for bbo_ticks (market sentiment). ts_exchange = Binance event time; ts_ingest = our processing time.
#[derive(Debug, Clone, Serialize, clickhouse::Row)]
pub struct BBOTickRow {
    pub ts_exchange: i64,
    pub ts_ingest: i64,
    pub update_id: u64,
    pub symbol: String,
    pub bid_price: f64,
    pub bid_qty: f64,
    pub ask_price: f64,
    pub ask_qty: f64,
}

/// NATS payload from market.depth.<symbol> (published by market-ingestor).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthSnapshot {
    pub symbol: String,
    pub ts_exchange: i64,
    pub ts_ingest: i64,
    pub bids_price: Vec<f64>,
    pub bids_qty: Vec<f64>,
    pub asks_price: Vec<f64>,
    pub asks_qty: Vec<f64>,
}

impl DepthSnapshot {
    pub fn to_row(&self) -> DepthSnapshotRow {
        DepthSnapshotRow {
            symbol: self.symbol.clone(),
            ts_exchange: self.ts_exchange,
            ts_ingest: self.ts_ingest,
            bids_price: self.bids_price.clone(),
            bids_qty: self.bids_qty.clone(),
            asks_price: self.asks_price.clone(),
            asks_qty: self.asks_qty.clone(),
        }
    }
}

/// L2 order book snapshot row for depth_snapshots (100ms snapshots; arrays for multi-level).
#[derive(Debug, Clone, Serialize, clickhouse::Row)]
pub struct DepthSnapshotRow {
    pub symbol: String,
    pub ts_exchange: i64,
    pub ts_ingest: i64,
    pub bids_price: Vec<f64>,
    pub bids_qty: Vec<f64>,
    pub asks_price: Vec<f64>,
    pub asks_qty: Vec<f64>,
}

/// NATS payload from market.liq.<symbol> (published by market-ingestor).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidationTick {
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub orig_qty: f64,
    pub last_filled_qty: f64,
    pub ts_exchange: i64,
    pub ts_ingest: i64,
}

impl LiquidationTick {
    pub fn to_row(&self) -> LiquidationTickRow {
        LiquidationTickRow {
            symbol: self.symbol.clone(),
            side: self.side.clone(),
            price: self.price,
            orig_qty: self.orig_qty,
            last_filled_qty: self.last_filled_qty,
            ts_exchange: self.ts_exchange,
            ts_ingest: self.ts_ingest,
        }
    }
}

/// Binance Futures force order (liquidation) row for liquidation_ticks.
#[derive(Debug, Clone, Serialize, clickhouse::Row)]
pub struct LiquidationTickRow {
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub orig_qty: f64,
    pub last_filled_qty: f64,
    pub ts_exchange: i64,
    pub ts_ingest: i64,
}

impl MarketEvent {
    /// Case A: Trade event (has p, q, t). ts_ingest_ms = system time when we received/parsed (UTC ms).
    pub fn to_trade_row(&self, ts_ingest_ms: i64) -> Option<TradeTickRow> {
        let ts_exchange = extract_ts_ms(&self.data)?;
        let trade_id = extract_u64(&self.data, "t")?;
        let price = extract_f64(&self.data, &["p", "price"])?;
        let quantity = extract_f64(&self.data, &["q", "quantity"])?;
        if !price.is_finite() || !quantity.is_finite() {
            return None;
        }
        let symbol = extract_symbol(&self.data)
            .or_else(|| self.stream.as_deref().and_then(parse_symbol_from_stream))
            .unwrap_or_else(|| "UNKNOWN".to_string());
        let is_buyer_maker = extract_bool(&self.data, "m").unwrap_or(false);
        Some(TradeTickRow {
            ts_exchange,
            ts_ingest: ts_ingest_ms,
            trade_id,
            symbol,
            price,
            quantity,
            is_buyer_maker,
        })
    }

    /// Case B: BBO/depth event (has b, a, u). ts_ingest_ms = system time when we received/parsed (UTC ms).
    pub fn to_bbo_row(&self, ts_ingest_ms: i64) -> Option<BBOTickRow> {
        let ts_exchange = extract_ts_ms(&self.data)?;
        let update_id = extract_u64(&self.data, "u")?;
        let bids = parse_levels(&self.data, "b").or_else(|| parse_levels(&self.data, "bids"))?;
        let asks = parse_levels(&self.data, "a").or_else(|| parse_levels(&self.data, "asks"))?;
        let (bid_price, bid_qty) = best_bid(&bids)?;
        let (ask_price, ask_qty) = best_ask(&asks)?;
        if !bid_price.is_finite() || !ask_price.is_finite() {
            return None;
        }
        let symbol = extract_symbol(&self.data)
            .or_else(|| self.stream.as_deref().and_then(parse_symbol_from_stream))
            .unwrap_or_else(|| "UNKNOWN".to_string());
        Some(BBOTickRow {
            ts_exchange,
            ts_ingest: ts_ingest_ms,
            update_id,
            symbol,
            bid_price,
            bid_qty,
            ask_price,
            ask_qty,
        })
    }
}

fn extract_symbol(data: &Value) -> Option<String> {
    data.get("s")
        .and_then(as_string_owned)
        .or_else(|| data.get("symbol").and_then(as_string_owned))
}

fn parse_symbol_from_stream(stream: &str) -> Option<String> {
    stream
        .split('@')
        .next()
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
}

fn extract_ts_ms(data: &Value) -> Option<i64> {
    data.get("T")
        .and_then(as_i64_loose)
        .or_else(|| data.get("E").and_then(as_i64_loose))
}

fn extract_f64(data: &Value, keys: &[&str]) -> Option<f64> {
    for k in keys {
        if let Some(v) = data.get(*k).and_then(as_f64_loose) {
            return Some(v);
        }
    }
    None
}

/// Extract u64 from key (Binance: number or string for tradeId / orderBookUpdateId).
fn extract_u64(data: &Value, key: &str) -> Option<u64> {
    let v = data.get(key)?;
    if let Some(n) = v.as_u64() {
        return Some(n);
    }
    if let Some(i) = v.as_i64() {
        return u64::try_from(i).ok();
    }
    if let Some(s) = v.as_str() {
        return s.parse::<u64>().ok();
    }
    None
}

fn extract_bool(data: &Value, key: &str) -> Option<bool> {
    let v = data.get(key)?;
    if let Some(b) = v.as_bool() {
        return Some(b);
    }
    if let Some(n) = v.as_u64() {
        return Some(n != 0);
    }
    if let Some(s) = v.as_str() {
        return Some(s == "1" || s.eq_ignore_ascii_case("true"));
    }
    None
}

fn parse_levels(data: &Value, key: &str) -> Option<Vec<(f64, f64)>> {
    let arr = data.get(key)?.as_array()?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        let level = item.as_array()?;
        if level.len() < 2 {
            continue;
        }
        let price = as_f64_loose(level.get(0)?)?;
        let qty = as_f64_loose(level.get(1)?)?;
        if price.is_finite() && qty.is_finite() {
            out.push((price, qty));
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn best_bid(levels: &[(f64, f64)]) -> Option<(f64, f64)> {
    levels
        .iter()
        .max_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal))
        .copied()
}

fn best_ask(levels: &[(f64, f64)]) -> Option<(f64, f64)> {
    levels
        .iter()
        .min_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal))
        .copied()
}

fn as_string_owned(v: &Value) -> Option<String> {
    v.as_str().map(|s| s.to_string())
}

fn as_f64_loose(v: &Value) -> Option<f64> {
    if let Some(x) = v.as_f64() {
        return Some(x);
    }
    if let Some(s) = v.as_str() {
        return s.parse::<f64>().ok();
    }
    None
}

fn as_i64_loose(v: &Value) -> Option<i64> {
    if let Some(x) = v.as_i64() {
        return Some(x);
    }
    if let Some(u) = v.as_u64() {
        return i64::try_from(u).ok();
    }
    if let Some(s) = v.as_str() {
        return s.parse::<i64>().ok();
    }
    None
}
