use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Normalize symbol to UPPERCASE for consistent HashMap lookups across market.raw and market.context.
pub fn norm_symbol(sym: &str) -> String {
    sym.to_uppercase()
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MarketEvent {
    pub stream: String,
    pub event_type: String,
    pub ts_ingest: String,
    pub data: Value,
}

impl MarketEvent {
    /// Try to extract the symbol from common fields.
    /// In our normalized format, trade payload includes `data.symbol`.
    /// Binance raw payload sometimes uses `s`.
    /// Returns symbol normalized to uppercase via norm_symbol().
    pub fn symbol(&self) -> Option<String> {
        let raw = self
            .data
            .get("symbol")
            .and_then(|v| v.as_str())
            .or_else(|| self.data.get("s").and_then(|v| v.as_str()))?;
        Some(norm_symbol(raw))
    }

    /// Try to extract price as f64 from common fields:
    /// normalized trade has `data.price`, Binance raw uses `p`.
    pub fn price_f64(&self) -> Option<f64> {
        let raw = if let Some(p) = self.data.get("price").and_then(|v| v.as_str()) {
            Some(p)
        } else {
            self.data.get("p").and_then(|v| v.as_str())
        };

        match raw {
            Some(s) => s.parse::<f64>().ok(),
            None => None,
        }
    }
}

/// Futures context event from NATS subject `market.context` (mark price, funding, OI).
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
