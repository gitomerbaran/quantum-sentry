use serde::{Deserialize, Deserializer, Serialize};

/// Deserialize u64 from either JSON number or string (Binance may send large IDs as strings).
fn deserialize_u64_number_or_string<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(u64),
        Str(String),
    }
    match NumOrStr::deserialize(deserializer)? {
        NumOrStr::Num(n) => Ok(n),
        NumOrStr::Str(s) => s.parse::<u64>().map_err(serde::de::Error::custom),
    }
}

/// Deserialize Option<u64> from missing, null, number, or string (Binance may omit b/a).
fn deserialize_option_u64_number_or_string<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OptNumOrStr {
        Num(u64),
        Str(String),
        Null,
    }
    let opt = Option::<OptNumOrStr>::deserialize(deserializer)?;
    match opt {
        None => Ok(None),
        Some(OptNumOrStr::Null) => Ok(None),
        Some(OptNumOrStr::Num(n)) => Ok(Some(n)),
        Some(OptNumOrStr::Str(s)) => s.parse::<u64>().map(Some).map_err(serde::de::Error::custom),
    }
}

/// Deserialize bool from JSON true/false or 0/1 (Binance may send "m" as 0/1).
fn deserialize_bool_number_or_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum BoolOrNum {
        Bool(bool),
        Num(u8),
    }
    match BoolOrNum::deserialize(deserializer)? {
        BoolOrNum::Bool(b) => Ok(b),
        BoolOrNum::Num(0) => Ok(false),
        BoolOrNum::Num(1) => Ok(true),
        BoolOrNum::Num(n) => Err(serde::de::Error::custom(format!(
            "expected 0/1 or bool, got {}",
            n
        ))),
    }
}

/// Deserialize String from JSON string or number (Binance may send price/qty as number).
fn deserialize_string_number_or_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StrOrNum {
        Str(String),
        Num(serde_json::Number),
    }
    match StrOrNum::deserialize(deserializer)? {
        StrOrNum::Str(s) => Ok(s),
        StrOrNum::Num(n) => Ok(n.to_string()),
    }
}

/// Binance REST endpoint: GET /api/v3/ticker/24hr
/// NOTE: Many numeric-looking fields are returned as strings.
/// We keep them as String unless they are clearly integer timestamps/ids.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Ticker24hr {
    pub symbol: String,

    #[serde(rename = "priceChange")]
    pub price_change: Option<String>,
    #[serde(rename = "priceChangePercent")]
    pub price_change_percent: Option<String>,
    #[serde(rename = "weightedAvgPrice")]
    pub weighted_avg_price: Option<String>,

    #[serde(rename = "prevClosePrice")]
    pub prev_close_price: Option<String>,
    #[serde(rename = "lastPrice")]
    pub last_price: Option<String>,
    #[serde(rename = "lastQty")]
    pub last_qty: Option<String>,

    #[serde(rename = "bidPrice")]
    pub bid_price: Option<String>,
    #[serde(rename = "bidQty")]
    pub bid_qty: Option<String>,
    #[serde(rename = "askPrice")]
    pub ask_price: Option<String>,
    #[serde(rename = "askQty")]
    pub ask_qty: Option<String>,

    #[serde(rename = "openPrice")]
    pub open_price: Option<String>,
    #[serde(rename = "highPrice")]
    pub high_price: Option<String>,
    #[serde(rename = "lowPrice")]
    pub low_price: Option<String>,

    /// Base asset volume (string in Binance)
    pub volume: String,
    /// Quote asset volume (string in Binance) - used for ranking
    #[serde(rename = "quoteVolume")]
    pub quote_volume: String,

    #[serde(rename = "openTime")]
    pub open_time: u64,
    #[serde(rename = "closeTime")]
    pub close_time: u64,

    #[serde(rename = "firstId")]
    pub first_id: Option<i64>,
    #[serde(rename = "lastId")]
    pub last_id: Option<i64>,
    pub count: Option<i64>,
}

/// Combined stream wrapper:
/// Binance combined streams wrap each message as:
/// { "stream": "<stream_name>", "data": { ...payload... } }
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CombinedStream<T> {
    pub stream: String,
    pub data: T,
}

/// Trade event payload for `<symbol>@trade`
/// Binance may send order IDs as number or string; "M" can be missing.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TradeEvent {
    /// Event type: "trade"
    #[serde(rename = "e")]
    pub event_type: String,
    /// Event time (ms since epoch); Binance may send as number or string.
    #[serde(rename = "E", deserialize_with = "deserialize_u64_number_or_string")]
    pub event_time: u64,
    /// Symbol, e.g. "BTCUSDT"
    #[serde(rename = "s")]
    pub symbol: String,

    /// Trade ID
    #[serde(rename = "t", deserialize_with = "deserialize_u64_number_or_string")]
    pub trade_id: u64,
    /// Price (string or number in JSON)
    #[serde(rename = "p", deserialize_with = "deserialize_string_number_or_string")]
    pub price: String,
    /// Quantity (string or number in JSON)
    #[serde(rename = "q", deserialize_with = "deserialize_string_number_or_string")]
    pub quantity: String,

    /// Buyer order ID (optional; Binance may omit in some streams)
    #[serde(
        rename = "b",
        default,
        deserialize_with = "deserialize_option_u64_number_or_string"
    )]
    pub buyer_order_id: Option<u64>,
    /// Seller order ID (optional; Binance may omit in some streams)
    #[serde(
        rename = "a",
        default,
        deserialize_with = "deserialize_option_u64_number_or_string"
    )]
    pub seller_order_id: Option<u64>,

    /// Trade time (ms since epoch); Binance may send as number or string.
    #[serde(rename = "T", deserialize_with = "deserialize_u64_number_or_string")]
    pub trade_time: u64,

    /// Is buyer the market maker? (Binance may send as true/false or 0/1)
    #[serde(rename = "m", deserialize_with = "deserialize_bool_number_or_bool")]
    pub is_buyer_maker: bool,

    /// Ignore field (Binance "M"); optional in some streams.
    #[serde(rename = "M", default)]
    pub ignore: bool,
}

/// A single depth level represented as (price, qty).
/// Binance sends arrays like ["123.45","0.678"] and serde can map them into tuples.
pub type Level = (String, String);

// ---------- Futures context (Binance Futures: mark price, funding rate, open interest) ----------

/// Deserialize i64 from JSON number (u64/i64) or string (Binance may send timestamps as either).
fn deserialize_i64_loose<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        N(i64),
        U(u64),
        S(String),
    }
    match NumOrStr::deserialize(deserializer)? {
        NumOrStr::N(n) => Ok(n),
        NumOrStr::U(u) => i64::try_from(u).map_err(serde::de::Error::custom),
        NumOrStr::S(s) => s.parse::<i64>().map_err(serde::de::Error::custom),
    }
}

fn deserialize_option_i64_loose<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<serde_json::Value>::deserialize(deserializer).and_then(|opt| {
        let v = match opt {
            None => return Ok(None),
            Some(v) => v,
        };
        let n = if let Some(i) = v.as_i64() {
            i
        } else if let Some(u) = v.as_u64() {
            i64::try_from(u).map_err(serde::de::Error::custom)?
        } else if let Some(s) = v.as_str() {
            s.parse::<i64>().map_err(serde::de::Error::custom)?
        } else {
            return Err(serde::de::Error::custom(
                "expected number or string for i64",
            ));
        };
        Ok(Some(n))
    })
}

/// Binance Futures mark price update payload: `<symbol>@markPrice@1s`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FuturesMarkPriceUpdate {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E", deserialize_with = "deserialize_i64_loose")]
    pub event_time: i64, // ms
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub mark_price: String,
    #[serde(rename = "r")]
    pub funding_rate: Option<String>,
    #[serde(
        rename = "T",
        default,
        deserialize_with = "deserialize_option_i64_loose"
    )]
    pub next_funding_time: Option<i64>, // ms
}

/// Binance Futures open interest payload: `<symbol>@openInterest@1s`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FuturesOpenInterestUpdate {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E", deserialize_with = "deserialize_i64_loose")]
    pub event_time: i64, // ms
    #[serde(rename = "s")]
    pub symbol: String,
    /// Binance may send "oi", "openInterest", or "o" (contracts).
    #[serde(rename = "oi", alias = "openInterest", alias = "o")]
    pub open_interest: String,
}

/// Normalized event for NATS subject `market.context` (ts_ingest captured at receive time).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MarketContextEvent {
    pub ts_exchange: i64, // ms
    pub ts_ingest: i64,   // ms
    pub symbol: String,
    pub mark_price: Option<f64>,
    pub funding_rate: Option<f64>,
    pub open_interest: Option<f64>,
    pub next_funding_time: Option<i64>, // ms
    pub source: String,                 // "futures"
}

/// Safe parse string to f64 for context fields.
pub fn parse_f64(s: &str) -> Option<f64> {
    s.parse::<f64>().ok().filter(|&x| x.is_finite())
}

/// Current time in epoch milliseconds (UTC). Prefer std::time for hot path.
pub fn now_ms() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(_) => 0,
    }
}

impl MarketContextEvent {
    /// Build from mark price update; use cached_open_interest when available (from prior openInterest events).
    pub fn from_mark(
        update: FuturesMarkPriceUpdate,
        ts_ingest: i64,
        cached_open_interest: Option<f64>,
    ) -> Self {
        Self {
            ts_exchange: update.event_time,
            ts_ingest,
            symbol: update.symbol,
            mark_price: parse_f64(&update.mark_price),
            funding_rate: update.funding_rate.and_then(|s| parse_f64(&s)),
            open_interest: cached_open_interest,
            next_funding_time: update.next_funding_time,
            source: "futures".to_string(),
        }
    }
}

// ---------- Futures L2 depth (depth5@100ms) + forceOrder (liquidation) ----------

/// Binance Futures depth5 update payload: `<symbol>@depth5@100ms`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FuturesDepthEvent {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E", deserialize_with = "deserialize_i64_loose")]
    pub event_time: i64, // ms
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    pub asks: Vec<[String; 2]>,
    #[serde(rename = "u", default)]
    pub update_id: Option<i64>,
}

/// Normalized depth snapshot for NATS (market.depth.<symbol>).
#[derive(Debug, Clone, Deserialize, Serialize)]
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
    pub fn from_depth5(raw: FuturesDepthEvent, ts_ingest: i64) -> Self {
        fn parse_pair(v: &[String; 2]) -> Option<(f64, f64)> {
            let p = v[0].parse::<f64>().ok()?;
            let q = v[1].parse::<f64>().ok()?;
            Some((p, q))
        }
        let mut bids_price = Vec::with_capacity(raw.bids.len());
        let mut bids_qty = Vec::with_capacity(raw.bids.len());
        for pair in &raw.bids {
            if let Some((p, q)) = parse_pair(pair) {
                if p.is_finite() && q.is_finite() {
                    bids_price.push(p);
                    bids_qty.push(q);
                }
            }
        }
        let mut asks_price = Vec::with_capacity(raw.asks.len());
        let mut asks_qty = Vec::with_capacity(raw.asks.len());
        for pair in &raw.asks {
            if let Some((p, q)) = parse_pair(pair) {
                if p.is_finite() && q.is_finite() {
                    asks_price.push(p);
                    asks_qty.push(q);
                }
            }
        }
        Self {
            symbol: raw.symbol.to_uppercase(),
            ts_exchange: raw.event_time,
            ts_ingest,
            bids_price,
            bids_qty,
            asks_price,
            asks_qty,
        }
    }
}

/// Binance Futures force order (liquidation) payload: `<symbol>@forceOrder`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FuturesForceOrderEvent {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E", deserialize_with = "deserialize_i64_loose")]
    pub event_time: i64,
    #[serde(rename = "o")]
    pub order: ForceOrderBody,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ForceOrderBody {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "q")]
    pub orig_qty: String,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "ap", default)]
    pub avg_price: Option<String>,
    #[serde(rename = "l", default)]
    pub last_filled_qty: Option<String>,
    #[serde(rename = "T", default)]
    pub trade_time: Option<i64>,
}

/// Normalized liquidation tick for NATS (market.liq.<symbol>).
#[derive(Debug, Clone, Deserialize, Serialize)]
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
    pub fn from_force_order(raw: FuturesForceOrderEvent, ts_ingest: i64) -> Option<Self> {
        let sym = raw.order.symbol.to_uppercase();
        let side = raw.order.side.clone();
        let price_str = raw.order.avg_price.as_deref().unwrap_or(&raw.order.price);
        let price = price_str.parse::<f64>().ok().filter(|&x| x.is_finite())?;
        let orig_qty = raw.order.orig_qty.parse::<f64>().ok().filter(|&x| x.is_finite())?;
        let last_filled_qty = raw
            .order
            .last_filled_qty
            .as_deref()
            .unwrap_or("0")
            .parse::<f64>()
            .ok()
            .filter(|&x| x.is_finite())?;
        let ts_exchange = raw.order.trade_time.unwrap_or(raw.event_time);
        Some(Self {
            symbol: sym,
            side,
            price,
            orig_qty,
            last_filled_qty,
            ts_exchange,
            ts_ingest,
        })
    }
}

// ---------- Spot depth ----------

/// Depth update payload for `<symbol>@depth@100ms`
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DepthUpdateEvent {
    /// Event type: "depthUpdate"
    #[serde(rename = "e")]
    pub event_type: String,
    /// Event time (ms since epoch)
    #[serde(rename = "E")]
    pub event_time: u64,
    /// Symbol, e.g. "BTCUSDT"
    #[serde(rename = "s")]
    pub symbol: String,

    /// First update ID in event
    #[serde(rename = "U")]
    pub first_update_id: u64,
    /// Final update ID in event
    #[serde(rename = "u")]
    pub final_update_id: u64,

    /// Bids to be updated
    #[serde(rename = "b")]
    pub bids: Vec<Level>,
    /// Asks to be updated
    #[serde(rename = "a")]
    pub asks: Vec<Level>,
}
