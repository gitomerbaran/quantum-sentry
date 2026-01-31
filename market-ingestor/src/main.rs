mod messaging;
mod model;
mod stream;
mod universe;

use anyhow::Result;
use reqwest::Client;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::{interval, Duration};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::messaging::NatsPublisher;
use crate::stream::StreamManager;

#[tokio::main]
async fn main() -> Result<()> {
    // Rustls 0.23+ requires a crypto provider; install ring before any TLS connection.
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|e| anyhow::anyhow!("rustls crypto provider: {:?}", e))?;

    init_tracing();

    info!("starting market-ingestor");

    let client = Client::new();

    // NATS publisher for market.raw (best-effort; service runs even if NATS is down).
    let nats = Arc::new(NatsPublisher::new(
        "nats://127.0.0.1:4222".to_string(),
        "market.raw".to_string(),
    ));

    // Watch channel for universe updates.
    // Sender holds the latest universe; Receiver reacts to changes.
    let (tx, rx) = watch::channel::<Vec<String>>(Vec::new());

    // Spawn Stream Task (it will wait until universe becomes non-empty).
    let nats_for_stream = nats.clone();
    let stream_task = tokio::spawn(async move {
        let mgr = StreamManager::new(nats_for_stream);
        if let Err(e) = mgr.run(rx).await {
            error!(error = %e, "stream manager exited with error");
        }
    });

    // Spawn Universe Task: immediate fetch at startup, then refresh every 15 minutes.
    let universe_client = client.clone();
    let universe_tx = tx.clone();
    let universe_task = tokio::spawn(async move {
        // Immediate fetch at startup (don't wait 15 minutes for first tick).
        if let Err(e) = refresh_universe_once(&universe_client, &universe_tx).await {
            warn!(error = %e, "initial universe fetch failed");
        }

        let mut tick = interval(Duration::from_secs(15 * 60));
        tick.tick().await; // consume immediate first tick so next tick is in 15 min
        loop {
            tick.tick().await;
            if let Err(e) = refresh_universe_once(&universe_client, &universe_tx).await {
                warn!(error = %e, "periodic universe fetch failed");
            }
        }
    });

    // Graceful shutdown on Ctrl+C
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("received Ctrl+C; shutting down");
        }
        _ = stream_task => {
            warn!("stream task finished unexpectedly");
        }
        _ = universe_task => {
            warn!("universe task finished unexpectedly");
        }
    }

    Ok(())
}

fn init_tracing() {
    // Respect RUST_LOG if set; otherwise default to info.
    // Example: RUST_LOG=info,market_ingestor=debug
    let filter = match EnvFilter::try_from_default_env() {
        Ok(f) => f,
        Err(_) => EnvFilter::new("info"),
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .compact()
        .init();
}

async fn refresh_universe_once(client: &Client, tx: &watch::Sender<Vec<String>>) -> Result<()> {
    let new_universe = universe::fetch_top_50_coins(client).await?;

    let current = tx.borrow().clone();
    if current != new_universe {
        // Log a compact diff summary
        info!(
            old_len = current.len(),
            new_len = new_universe.len(),
            sample_new = ?new_universe.iter().take(5).collect::<Vec<_>>(),
            "universe changed -> broadcasting update"
        );
        // If receiver dropped, send returns error; we can ignore and let tasks exit.
        let _ = tx.send(new_universe);
    } else {
        info!("universe unchanged");
    }

    Ok(())
}
