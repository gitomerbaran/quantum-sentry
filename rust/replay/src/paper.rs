//! Paper broker integration for replay: deterministic execution of OrderCommands using BBO prices.

use common::BboTick;
use sim_broker::{AccountSnapshot, Fill, PaperBroker, PaperBrokerConfig};
use std::collections::BTreeMap;

/// Result of paper trading simulation.
#[derive(Debug, Default)]
pub struct PaperResult {
    pub initial_cash_usdt: f64,
    pub fills: Vec<Fill>,
    pub account_snapshots: Vec<AccountSnapshot>,
}

/// Run paper trading simulation: execute OrderCommands using BBO prices.
/// Deterministic: same inputs => same outputs.
/// orders_with_decisions: OrderCommand with its corresponding Decision (for ts_exchange).
pub fn run_paper_trading(
    bbo_ticks: &[BboTick],
    orders_with_decisions: &[crate::engine::OrderWithDecision],
    config: PaperBrokerConfig,
) -> PaperResult {
    let initial_cash_usdt = config.initial_cash_usdt;
    let mut broker = PaperBroker::new(config);

    // Build sorted event list: (ts_exchange, event_type, data)
    // event_type: 0 = BBO, 1 = Order
    // For ties (same ts_exchange), process BBO first (type 0 < 1)
    let mut events: Vec<(i64, u8, usize)> = Vec::new();
    for (idx, bbo) in bbo_ticks.iter().enumerate() {
        events.push((bbo.ts_exchange, 0, idx));
    }
    for (idx, ord_with_dec) in orders_with_decisions.iter().enumerate() {
        events.push((ord_with_dec.decision.ts_exchange, 1, idx));
    }
    events.sort();

    let mut fills = Vec::new();
    let mut account_snapshots = Vec::new();
    let mut last_snapshot_ts: BTreeMap<String, i64> = BTreeMap::new();
    const SNAPSHOT_INTERVAL_MS: i64 = 1000; // Snapshot every 1 second per symbol

    for (ts_exchange, event_type, idx) in events {
        match event_type {
            0 => {
                // BBO update
                let bbo = &bbo_ticks[idx];
                broker.update_bbo(bbo);

                // Generate snapshot if interval passed
                let symbol = bbo.symbol.to_uppercase();
                let last_ts = last_snapshot_ts.get(&symbol).copied().unwrap_or(0);
                if ts_exchange - last_ts >= SNAPSHOT_INTERVAL_MS {
                    if let Some(snapshot) = broker.account_snapshot(&symbol, ts_exchange) {
                        account_snapshots.push(snapshot);
                        last_snapshot_ts.insert(symbol, ts_exchange);
                    }
                }
            }
            1 => {
                // Order execution
                let ord_with_dec = &orders_with_decisions[idx];
                let fill = broker.execute_order(&ord_with_dec.order, ts_exchange);
                fills.push(fill.clone());

                // Generate snapshot after fill
                let symbol = ord_with_dec.order.symbol.to_uppercase();
                if let Some(snapshot) = broker.account_snapshot(&symbol, ts_exchange) {
                    account_snapshots.push(snapshot);
                    last_snapshot_ts.insert(symbol, ts_exchange);
                }
            }
            _ => unreachable!(),
        }
    }

    // Final snapshots for all active symbols
    for symbol in broker.active_symbols() {
        if let Some(last_bbo) = bbo_ticks.last() {
            if let Some(snapshot) = broker.account_snapshot(&symbol, last_bbo.ts_exchange) {
                account_snapshots.push(snapshot);
            }
        }
    }

    PaperResult {
        initial_cash_usdt,
        fills,
        account_snapshots,
    }
}

