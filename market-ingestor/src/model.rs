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
