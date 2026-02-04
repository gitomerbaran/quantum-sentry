//! Minimal wiring: subscribe to NATS market.raw, parse BBO, feed FeatureEngine, emit snapshots.
//! No DB reads. No Executor/Orders. Bounded channels, drop-old when full.

use common::BboTick;
use feature_engine::runner;
use futures_util::StreamExt;
use tracing::{info, warn};

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Parse NATS market.raw payload: if BBO (has b, a, u), return BboTick with ts_ingest.
fn parse_bbo_from_payload(payload: &[u8], ts_ingest: i64) -> Option<BboTick> {
    let root: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let data = root.get("data").or_else(|| root.get("d"))?;
    let b_arr = data.get("b").or_else(|| data.get("bids"))?.as_array()?;
    let a_arr = data.get("a").or_else(|| data.get("asks"))?.as_array()?;
    if b_arr.is_empty() || a_arr.is_empty() {
        return None;
    }
    let first_bid = b_arr.first()?.as_array()?;
    let first_ask = a_arr.first()?.as_array()?;
    let bid_price = as_f64(first_bid.first()?)?;
    let bid_qty = as_f64(first_bid.get(1)?)?;
    let ask_price = as_f64(first_ask.first()?)?;
    let ask_qty = as_f64(first_ask.get(1)?)?;
    let ts_exchange = data
        .get("T")
        .and_then(as_i64)
        .or_else(|| data.get("E").and_then(as_i64))?;
    let update_id = data.get("u").and_then(as_u64)?;
    let symbol = data
        .get("s")
        .or_else(|| data.get("symbol"))?
        .as_str()?
        .to_uppercase();
    Some(BboTick {
        ts_exchange,
        ts_ingest,
        update_id,
        symbol,
        bid_price,
        bid_qty,
        ask_price,
        ask_qty,
    })
}

fn as_f64(v: &serde_json::Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

fn as_i64(v: &serde_json::Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_u64().and_then(|u| i64::try_from(u).ok()))
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

fn as_u64(v: &serde_json::Value) -> Option<u64> {
    v.as_u64()
        .or_else(|| v.as_i64().and_then(|i| u64::try_from(i).ok()))
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .compact()
        .init();

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let subject = std::env::var("NATS_SUBJECT").unwrap_or_else(|_| "market.raw".to_string());
    let window: usize = std::env::var("FEATURE_WINDOW")
        .unwrap_or_else(|_| "60".to_string())
        .parse()
        .unwrap_or(60);

    let (tx_bbo, rx_bbo, tx_snap, mut rx_snap) = runner::channel_pair(50_000);

    let engine_handle = tokio::spawn(async move {
        runner::run(rx_bbo, tx_snap, window).await;
    });

    let nats_url2 = nats_url.clone();
    let subject2 = subject.clone();
    let ingest_handle = tokio::spawn(async move {
        loop {
            match async_nats::connect(&nats_url2).await {
                Ok(client) => {
                    info!(url = %nats_url2, "connected to NATS");
                    if let Ok(mut sub) = client.subscribe(subject2.clone()).await {
                        info!(subject = %subject2, "subscribed");
                        while let Some(msg) = sub.next().await {
                            let ts_ingest = now_ms();
                            if let Some(tick) = parse_bbo_from_payload(&msg.payload, ts_ingest) {
                                if tx_bbo.send(tick).await.is_err() {
                                    warn!("BBO channel closed");
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, "NATS connect failed; retry in 2s");
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                }
            }
        }
    });

    let consumer_handle = tokio::spawn(async move {
        let mut count: u64 = 0;
        while let Some(snap) = rx_snap.recv().await {
            count += 1;
            if count <= 5 || count.is_multiple_of(10_000) {
                info!(
                    count,
                    symbol = %snap.symbol,
                    ready = snap.ready,
                    features_len = snap.features.len(),
                    "snapshot"
                );
            }
        }
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl+C; shutting down");
        }
        _ = ingest_handle => {}
        _ = engine_handle => {}
        _ = consumer_handle => {}
    }

    Ok(())
}
