mod listener;
mod model;
mod paper_broker;
mod strategy;
mod wallet;

#[cfg(test)]
mod tests;

use anyhow::Result;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::strategy::{Strategy, TrendFollowStrategy};
use crate::wallet::Wallet;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    info!("starting sim-broker (futures simulator)");

    let wallet = Wallet::new();
    wallet.log_status();

    let strategy: Box<dyn Strategy> = Box::new(TrendFollowStrategy::new());

    let listener_task = tokio::spawn(async move {
        if let Err(e) = listener::run_listener(wallet, strategy).await {
            tracing::warn!(error = %e, "listener exited with error");
        }
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("received Ctrl+C; shutting down");
        }
        _ = listener_task => {
            tracing::warn!("listener task finished unexpectedly");
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
