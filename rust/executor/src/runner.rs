//! Bounded channel runner: FeatureSnapshot in -> Executor -> Decision (+ optional OrderCommand) out.
//! Optional cold-path logger: try_send LogEvent (Decision, OrderCommand) so hot path never blocks.

use common::{Decision, FeatureSnapshot, LogEvent, OrderCommand};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

use crate::config::ExecutorConfig;
use crate::executor::Executor;
use crate::inference::InferenceEngine;
use logger::LoggerCounters;

const DECISION_CHANNEL_CAP: usize = 1024;
const ORDER_CHANNEL_CAP: usize = 512;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Run executor: consume snapshots from rx_snapshot, send Decision to tx_decision,
/// and optionally OrderCommand to tx_order when execution_enabled.
/// If tx_log/counters provided, try_send LogEvent (non-blocking); drop on full per config.
#[allow(clippy::too_many_arguments)]
pub async fn run<E: InferenceEngine + 'static>(
    mut rx_snapshot: mpsc::Receiver<FeatureSnapshot>,
    tx_decision: mpsc::Sender<Decision>,
    tx_order: Option<mpsc::Sender<OrderCommand>>,
    config: ExecutorConfig,
    engine: E,
    tx_log: Option<mpsc::Sender<LogEvent>>,
    logger_drop_on_full: bool,
    logger_counters: Option<Arc<LoggerCounters>>,
) {
    let mut executor = Executor::new(config, engine);
    while let Some(snap) = rx_snapshot.recv().await {
        let (dec, order_opt) = executor.on_snapshot(&snap);
        if let (Some(ref tx), Some(ref cnt)) = (&tx_log, &logger_counters) {
            let ts = now_ms();
            logger::try_send_log(
                tx,
                LogEvent::Decision {
                    ts_ingest: ts,
                    dec: dec.clone(),
                },
                logger_drop_on_full,
                cnt,
            );
        }
        match tx_decision.try_send(dec) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(d)) => {
                warn!(symbol = %d.symbol, "decision channel full; dropping");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => break,
        }
        if let Some(cmd) = order_opt {
            let cmd_for_log = cmd.clone();
            if let Some(ref tx) = tx_order {
                if tx.try_send(cmd).is_err() {
                    warn!("order channel full; dropping order command");
                }
            }
            if let (Some(ref tx), Some(ref cnt)) = (&tx_log, &logger_counters) {
                logger::try_send_log(
                    tx,
                    LogEvent::OrderCommand {
                        ts_ingest: now_ms(),
                        cmd: cmd_for_log,
                    },
                    logger_drop_on_full,
                    cnt,
                );
            }
        }
    }
    tracing::debug!("snapshot channel closed; stopping executor runner");
}

/// Create bounded channels for Decision and optional OrderCommand.
pub fn channel_pair() -> (
    mpsc::Sender<Decision>,
    mpsc::Receiver<Decision>,
    mpsc::Sender<OrderCommand>,
    mpsc::Receiver<OrderCommand>,
) {
    let (tx_dec, rx_dec) = mpsc::channel(DECISION_CHANNEL_CAP);
    let (tx_ord, rx_ord) = mpsc::channel(ORDER_CHANNEL_CAP);
    (tx_dec, rx_dec, tx_ord, rx_ord)
}
