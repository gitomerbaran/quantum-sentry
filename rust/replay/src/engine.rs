//! Deterministic replay loop: source -> feature_engine -> executor -> collected outputs.

use super::{ReplayEvent, ReplaySource};
use common::{BboTick, Decision, FeatureSnapshot, OrderCommand};
use executor::{run as executor_run, BoxedInferenceEngine, ExecutorConfig};
use feature_engine::runner;
use logger::LoggerCounters;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Result of a replay run: BBOs sent, snapshots and decisions (and optional orders) collected.
#[derive(Debug, Default)]
pub struct ReplayResult {
    /// BBO ticks sent in order (for golden raw_bbo.jsonl).
    pub bbo_ticks: Vec<BboTick>,
    pub snapshots: Vec<FeatureSnapshot>,
    pub decisions: Vec<Decision>,
    pub orders: Vec<OrderCommand>,
}

/// Run deterministic replay: fetch events from source, feed BBOs into feature_engine,
/// relay snapshots to executor, collect decisions (and orders if execution_enabled).
/// replay_speed: 0 = as-fast-as-possible; >0 = real-time multiplier (sleep by ts_exchange delta / speed).
pub async fn run_replay(
    source: &(dyn ReplaySource + Send + Sync),
    window: usize,
    executor_config: ExecutorConfig,
    engine: BoxedInferenceEngine,
    replay_speed: f64,
    execution_enabled: bool,
) -> anyhow::Result<ReplayResult> {
    let events = source.fetch_events().await?;
    if events.is_empty() {
        return Ok(ReplayResult::default());
    }

    let (tx_bbo, rx_bbo, tx_snap_fe, mut rx_snap_fe) = runner::channel_pair(50_000);
    let (tx_snap_ex, rx_snap_ex) = mpsc::channel::<FeatureSnapshot>(1024);
    let (tx_dec, mut rx_dec, tx_ord, mut rx_ord) = executor::runner::channel_pair();

    let tx_order_opt = if execution_enabled {
        Some(tx_ord)
    } else {
        None
    };

    let fe_handle = tokio::spawn(async move {
        runner::run(rx_bbo, tx_snap_fe, window).await;
    });

    let relay_handle = tokio::spawn(async move {
        let mut snapshots = Vec::new();
        while let Some(snap) = rx_snap_fe.recv().await {
            if tx_snap_ex.send(snap.clone()).await.is_err() {
                break;
            }
            snapshots.push(snap);
        }
        drop(tx_snap_ex);
        snapshots
    });

    let exec_handle = tokio::spawn(async move {
        executor_run(
            rx_snap_ex,
            tx_dec,
            tx_order_opt,
            executor_config,
            engine,
            None,
            false,
            None::<Arc<LoggerCounters>>,
        )
        .await;
    });

    let dec_handle = tokio::spawn(async move {
        let mut decisions = Vec::new();
        while let Some(d) = rx_dec.recv().await {
            decisions.push(d);
        }
        decisions
    });

    let ord_handle = tokio::spawn(async move {
        let mut orders = Vec::new();
        while let Some(o) = rx_ord.recv().await {
            orders.push(o);
        }
        orders
    });

    let mut bbo_ticks = Vec::new();
    let mut prev_ts: Option<i64> = None;
    for event in events {
        if let ReplayEvent::Bbo(tick) = event {
            if replay_speed > 0.0 {
                if let Some(prev) = prev_ts {
                    let delta_ms = (tick.ts_exchange - prev).max(0);
                    if delta_ms > 0 {
                        let sleep_ms = (delta_ms as f64 / replay_speed) as u64;
                        tokio::time::sleep(tokio::time::Duration::from_millis(sleep_ms)).await;
                    }
                }
                prev_ts = Some(tick.ts_exchange);
            }
            bbo_ticks.push(tick.clone());
            if tx_bbo.send(tick).await.is_err() {
                break;
            }
        }
    }
    drop(tx_bbo);

    let (snapshots, decisions, orders) = tokio::try_join!(relay_handle, dec_handle, ord_handle)?;
    fe_handle.await?;
    exec_handle.await?;

    Ok(ReplayResult {
        bbo_ticks,
        snapshots,
        decisions,
        orders,
    })
}
