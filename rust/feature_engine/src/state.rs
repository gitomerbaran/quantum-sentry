//! Per-symbol state: ring buffer + last spread_pct and imbalance from BBO.

use crate::features::{imbalance, log_return_1, log_return_window, spread_pct};
use crate::ring_buffer::RingBuffer;
use common::BboTick;
use common::FeatureSnapshot;
use std::collections::HashMap;
use tracing::debug;

const FEATURE_VERSION: u32 = 1;
/// Default ring buffer capacity (number of mid prices).
const DEFAULT_WINDOW: usize = 60;

/// Per-symbol state: ring buffer of mids + last BBO-derived spread and imbalance.
#[derive(Debug)]
pub struct PerSymbolState {
    pub symbol: String,
    ring: RingBuffer,
    last_spread_pct: Option<f32>,
    last_imbalance: f32,
}

impl PerSymbolState {
    pub fn new(symbol: String, window: usize) -> Self {
        Self {
            symbol,
            ring: RingBuffer::new(window),
            last_spread_pct: None,
            last_imbalance: 0.0,
        }
    }

    /// Update from BBO: compute mid, spread_pct, imbalance; push mid to ring; optionally produce FeatureSnapshot.
    pub fn on_bbo(&mut self, tick: &BboTick) -> Option<FeatureSnapshot> {
        let mid = (tick.bid_price + tick.ask_price) / 2.0;
        if !mid.is_finite() || mid <= 0.0 {
            debug!(symbol = %self.symbol, "invalid mid from BBO; skipping");
            return None;
        }

        let spread = spread_pct(tick.bid_price, tick.ask_price);
        self.last_spread_pct = spread;
        self.last_imbalance = imbalance(tick.bid_qty, tick.ask_qty);

        let prev_mid = self.ring.last().map(|(m, _)| m);
        self.ring.push(mid, tick.ts_exchange);

        if !self.ring.is_ready() {
            return None;
        }

        let (last_mid, ts_exchange) = self.ring.last().unwrap();
        let first_mid = self.ring.first().map(|(m, _)| m).unwrap_or(last_mid);

        let lr1 = prev_mid.and_then(|pm| log_return_1(last_mid, pm));
        let lr_window = log_return_window(last_mid, first_mid);

        let features: Vec<f32> = vec![
            lr1.unwrap_or(0.0),
            lr_window.unwrap_or(0.0),
            self.last_spread_pct.unwrap_or(0.0),
            self.last_imbalance,
        ];

        let d = tick.ts_ingest - tick.ts_exchange;
        let data_latency_ms: u32 = if d < 0 {
            0
        } else {
            d.min(i64::from(u32::MAX)) as u32
        };

        let ready = lr1.is_some() && self.last_spread_pct.is_some();

        Some(FeatureSnapshot {
            ts_exchange,
            symbol: self.symbol.clone(),
            features,
            ready,
            data_latency_ms,
            spread_pct: self.last_spread_pct.unwrap_or(0.0),
            feature_version: FEATURE_VERSION,
        })
    }
}

/// Feature engine: per-symbol state, consumes BboTick, produces FeatureSnapshot.
#[derive(Debug)]
pub struct FeatureEngine {
    state: HashMap<String, PerSymbolState>,
    window: usize,
}

impl FeatureEngine {
    pub fn new(window: usize) -> Self {
        Self {
            state: HashMap::new(),
            window,
        }
    }

    pub fn with_default_window() -> Self {
        Self::new(DEFAULT_WINDOW)
    }

    fn state_for(&mut self, symbol: &str) -> &mut PerSymbolState {
        if !self.state.contains_key(symbol) {
            self.state.insert(
                symbol.to_string(),
                PerSymbolState::new(symbol.to_string(), self.window),
            );
        }
        self.state.get_mut(symbol).unwrap()
    }

    /// Process one BBO tick; returns FeatureSnapshot when window is ready (optional).
    pub fn on_bbo(&mut self, tick: &BboTick) -> Option<FeatureSnapshot> {
        let symbol = tick.symbol.clone();
        self.state_for(&symbol).on_bbo(tick)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;

    fn bbo(
        ts_ex: i64,
        ts_ing: i64,
        symbol: &str,
        bid: f64,
        bid_q: f64,
        ask: f64,
        ask_q: f64,
    ) -> BboTick {
        BboTick {
            ts_exchange: ts_ex,
            ts_ingest: ts_ing,
            update_id: 0,
            symbol: symbol.to_string(),
            bid_price: bid,
            bid_qty: bid_q,
            ask_price: ask,
            ask_qty: ask_q,
        }
    }

    #[test]
    fn spread_pct_from_engine() {
        let mut engine = FeatureEngine::new(2);
        let t1 = bbo(1, 2, "X", 99.0, 1.0, 101.0, 1.0);
        let t2 = bbo(2, 3, "X", 99.0, 1.0, 101.0, 1.0);
        let _ = engine.on_bbo(&t1);
        let snap = engine.on_bbo(&t2).unwrap();
        assert!(snap.ready);
        assert_relative_eq!(snap.spread_pct, 0.02f32, epsilon = 1e-5);
    }

    #[test]
    fn imbalance_from_engine() {
        let mut engine = FeatureEngine::new(2);
        let t1 = bbo(1, 2, "X", 100.0, 3.0, 101.0, 1.0);
        let t2 = bbo(2, 3, "X", 100.0, 3.0, 101.0, 1.0);
        let _ = engine.on_bbo(&t1);
        let snap = engine.on_bbo(&t2).unwrap();
        assert_eq!(snap.features.len(), 4);
        assert_relative_eq!(snap.features[3], 0.5f32, epsilon = 1e-5); // imbalance
    }
}
