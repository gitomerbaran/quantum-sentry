//! ReplaySource abstraction: ClickHouse or JSONL file. Merged stream sorted by ts_exchange.

use common::BboTick;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Unified event for replay. Sorted by ts_exchange, then by update_id/exchange_id for stability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplayEvent {
    Bbo(BboTick),
    Trade(common::TradeTick),
}

impl ReplayEvent {
    pub fn ts_exchange(&self) -> i64 {
        match self {
            ReplayEvent::Bbo(t) => t.ts_exchange,
            ReplayEvent::Trade(t) => t.ts_exchange,
        }
    }

    pub fn symbol(&self) -> &str {
        match self {
            ReplayEvent::Bbo(t) => &t.symbol,
            ReplayEvent::Trade(t) => &t.symbol,
        }
    }

    /// Tie-breaker for stable sort: BBO uses update_id, Trade uses exchange_id.
    pub fn order_key(&self) -> u64 {
        match self {
            ReplayEvent::Bbo(t) => t.update_id,
            ReplayEvent::Trade(t) => t.exchange_id,
        }
    }
}

impl PartialEq for ReplayEvent {
    fn eq(&self, other: &Self) -> bool {
        self.ts_exchange() == other.ts_exchange() && self.order_key() == other.order_key()
    }
}

impl Eq for ReplayEvent {}

impl PartialOrd for ReplayEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReplayEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ts_exchange()
            .cmp(&other.ts_exchange())
            .then_with(|| self.order_key().cmp(&other.order_key()))
    }
}

/// Source of replay events in time order. Offline only (no hot path).
#[async_trait::async_trait]
pub trait ReplaySource: Send {
    /// Fetch all events for the configured symbol/range, sorted by ts_exchange (and order_key).
    async fn fetch_events(&self) -> anyhow::Result<Vec<ReplayEvent>>;
}
