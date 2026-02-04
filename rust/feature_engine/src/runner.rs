//! Bounded channel runner: BBO in -> FeatureEngine -> FeatureSnapshot out.
//! Drop-old keep-latest: if snapshot channel is full, try_send fails and we drop (log).

use common::{BboTick, FeatureSnapshot};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::state::FeatureEngine;

/// Capacity for snapshot channel (backpressure: drop when full).
const SNAPSHOT_CHANNEL_CAP: usize = 1024;

/// Run feature engine: consume BBO from `rx_bbo`, send snapshots to `tx_snapshot`.
/// If `tx_snapshot` is full, snapshot is dropped (drop-old keep-latest).
pub async fn run(
    mut rx_bbo: mpsc::Receiver<BboTick>,
    tx_snapshot: mpsc::Sender<FeatureSnapshot>,
    window: usize,
) {
    let mut engine = FeatureEngine::new(window);
    while let Some(tick) = rx_bbo.recv().await {
        if let Some(snap) = engine.on_bbo(&tick) {
            match tx_snapshot.try_send(snap) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(snap)) => {
                    warn!(
                        symbol = %snap.symbol,
                        "snapshot channel full; dropping snapshot (drop-old keep-latest)"
                    );
                    drop(snap);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    debug!("snapshot channel closed; stopping runner");
                    break;
                }
            }
        }
    }
}

/// Create bounded channel pair for BBO -> Runner -> Snapshots.
/// Returns (tx_bbo, rx_bbo, tx_snapshot, rx_snapshot).
pub fn channel_pair(
    bbo_cap: usize,
) -> (
    mpsc::Sender<BboTick>,
    mpsc::Receiver<BboTick>,
    mpsc::Sender<FeatureSnapshot>,
    mpsc::Receiver<FeatureSnapshot>,
) {
    let (tx_bbo, rx_bbo) = mpsc::channel(bbo_cap);
    let (tx_snap, rx_snap) = mpsc::channel(SNAPSHOT_CHANNEL_CAP);
    (tx_bbo, rx_bbo, tx_snap, rx_snap)
}
