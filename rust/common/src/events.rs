//! Shared event types for Quantum Sentinel v2 (single source of truth).
//! All timestamps: i64 epoch millis (UTC).

use serde::{Deserialize, Serialize};

use crate::types::{Action, OrderSide, OrderType};

/// Executed trade tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeTick {
    pub ts_exchange: i64,
    pub ts_ingest: i64,
    pub exchange_id: u64,
    pub symbol: String,
    pub price: f64,
    pub qty: f64,
    pub is_buyer_maker: bool,
}

/// Best bid/offer tick.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BboTick {
    pub ts_exchange: i64,
    pub ts_ingest: i64,
    pub update_id: u64,
    pub symbol: String,
    pub bid_price: f64,
    pub bid_qty: f64,
    pub ask_price: f64,
    pub ask_qty: f64,
}

/// Feature vector snapshot (scale-invariant features; ready for model).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureSnapshot {
    pub ts_exchange: i64,
    pub symbol: String,
    pub features: Vec<f32>,
    pub ready: bool,
    pub data_latency_ms: u32,
    pub spread_pct: f32,
    pub feature_version: u32,
}

/// Model/signal decision (no DB on hot path).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    pub ts_exchange: i64,
    pub symbol: String,
    pub action: Action,
    pub confidence: f32,
    pub reason_codes: Vec<String>,
    pub model_version: String,
}

/// Order command (idempotency via client_order_id).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCommand {
    pub client_order_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub qty: f64,
    pub price: Option<f64>,
    pub reduce_only: Option<bool>,
    pub leverage: Option<u32>,
    pub sl: Option<f64>,
    pub tp: Option<f64>,
}

/// Cold-path log event: ts_ingest = when we ingested; payload has ts_exchange/symbol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogEvent {
    TradeTick {
        ts_ingest: i64,
        tick: TradeTick,
    },
    BboTick {
        ts_ingest: i64,
        tick: BboTick,
    },
    FeatureSnapshot {
        ts_ingest: i64,
        snap: FeatureSnapshot,
    },
    Decision {
        ts_ingest: i64,
        dec: Decision,
    },
    OrderCommand {
        ts_ingest: i64,
        cmd: OrderCommand,
    },
    /// Placeholder for later (fill/fill report).
    ExecutionResult,
}
