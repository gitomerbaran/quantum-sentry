mod listener;
mod model;
mod strategy;
mod wallet;

use std::sync::Arc;
use tokio::sync::Mutex;

use anyhow::Result;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::wallet::Wallet;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    info!("starting sim-broker");

    let wallet = Arc::new(Mutex::new(Wallet::new()));
    {
        let w = wallet.lock().await;
        w.log_status();
    }

    let wallet_clone = wallet.clone();
    let listener_task = tokio::spawn(async move {
        if let Err(e) = listener::run_listener(wallet_clone).await {
            warn!(error = %e, "listener exited with error");
        }
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("received Ctrl+C; shutting down");
        }
        _ = listener_task => {
            warn!("listener task finished unexpectedly");
        }
    }

    Ok(())
}

fn init_tracing() {
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
