use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    pub fn symbol(&self) -> Option<String> {
        if let Some(s) = self.data.get("symbol").and_then(|v| v.as_str()) {
            return Some(s.to_string());
        }
        if let Some(s) = self.data.get("s").and_then(|v| v.as_str()) {
            return Some(s.to_string());
        }
        None
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
