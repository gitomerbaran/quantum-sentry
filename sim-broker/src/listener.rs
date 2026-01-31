use anyhow::Result;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tracing::{debug, info, warn};

use crate::model::MarketEvent;
use crate::strategy::{self, Action};
use crate::wallet::Wallet;

const NATS_URL: &str = "nats://127.0.0.1:4222";
const SUBJECT: &str = "market.raw";

pub async fn run_listener(wallet: Arc<Mutex<Wallet>>) -> Result<()> {
    let mut backoff = Duration::from_millis(250);
    let max_backoff = Duration::from_secs(10);

    loop {
        match async_nats::connect(NATS_URL).await {
            Ok(client) => {
                info!(server = %NATS_URL, subject = %SUBJECT, "connected to NATS; subscribing");
                backoff = Duration::from_millis(250);

                let sub = match client.subscribe(SUBJECT.to_string()).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(error = %e, "failed to subscribe; will reconnect");
                        sleep(backoff).await;
                        backoff = std::cmp::min(backoff * 2, max_backoff);
                        continue;
                    }
                };

                let mut messages = sub;
                while let Some(msg) = messages.next().await {
                    let payload = msg.payload;

                    let evt: MarketEvent = match serde_json::from_slice(&payload) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(error = %e, "failed to deserialize MarketEvent; skipping message");
                            continue;
                        }
                    };

                    let symbol = match evt.symbol() {
                        Some(s) => s,
                        None => continue,
                    };

                    let price = match evt.price_f64() {
                        Some(p) => p,
                        None => continue,
                    };

                    debug!(symbol = %symbol, price = price, "received price event");

                    let mut w = wallet.lock().await;
                    w.update_mark(&symbol, price);

                    let action = strategy::evaluate(&w, &symbol, price);
                    let did_trade = match action {
                        Action::Buy(usdt_amount) => {
                            w.execute_buy(&symbol, price, usdt_amount);
                            true
                        }
                        Action::Sell => {
                            w.execute_sell(&symbol, price);
                            true
                        }
                        Action::Hold => false,
                    };

                    if did_trade {
                        w.log_status();
                    }
                }

                warn!("NATS subscription stream ended; reconnecting");
            }
            Err(e) => {
                warn!(
                    error = %e,
                    server = %NATS_URL,
                    backoff_ms = backoff.as_millis(),
                    "failed to connect to NATS; retrying"
                );
                sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, max_backoff);
            }
        }
    }
}
