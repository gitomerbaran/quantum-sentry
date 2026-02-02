//! Multi-stream NATS listener: market.raw (trades) and market.context (mark/funding/OI).
//! Single-threaded hot loop with tokio::select!; funding applied on each message when due.

use anyhow::Result;
use futures_util::StreamExt;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration};
use tracing::{debug, info, warn};

use crate::model::{MarketContextEvent, MarketEvent};
use crate::strategy::{symbol_context_from_wallet, Strategy, TradeAction};
use crate::wallet::Wallet;

const NATS_URL: &str = "nats://127.0.0.1:4222";
const SUBJECT_RAW: &str = "market.raw";
const SUBJECT_CONTEXT: &str = "market.context";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub async fn run_listener(mut wallet: Wallet, mut strategy: Box<dyn Strategy>) -> Result<()> {
    let mut backoff = Duration::from_millis(250);
    let max_backoff = Duration::from_secs(10);
    let mut tick_count: u64 = 0;

    loop {
        let client = match async_nats::connect(NATS_URL).await {
            Ok(c) => {
                info!(server = %NATS_URL, "connected to NATS");
                backoff = Duration::from_millis(250);
                c
            }
            Err(e) => {
                warn!(
                    error = %e,
                    server = %NATS_URL,
                    backoff_ms = backoff.as_millis(),
                    "NATS connect failed; retrying"
                );
                sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, max_backoff);
                continue;
            }
        };

        let raw_sub = match client.subscribe(SUBJECT_RAW.to_string()).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "subscribe market.raw failed");
                sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, max_backoff);
                continue;
            }
        };
        let ctx_sub = match client.subscribe(SUBJECT_CONTEXT.to_string()).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "subscribe market.context failed");
                sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, max_backoff);
                continue;
            }
        };

        info!(
            raw = %SUBJECT_RAW,
            context = %SUBJECT_CONTEXT,
            "subscribed to both streams"
        );

        let mut raw_stream = raw_sub;
        let mut ctx_stream = ctx_sub;

        loop {
            tokio::select! {
                msg = raw_stream.next() => {
                    let Some(msg) = msg else {
                        warn!("market.raw stream ended");
                        break;
                    };
                    let payload = msg.payload;
                    let evt: MarketEvent = match serde_json::from_slice(&payload) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(error = %e, "deserialize MarketEvent failed; skip");
                            continue;
                        }
                    };
                    let symbol = match evt.symbol() {
                        Some(s) => s,
                        None => {
                            debug!("market.raw event missing symbol; skip");
                            continue;
                        }
                    };
                    let price = match evt.price_f64() {
                        Some(p) if p > 0.0 && p.is_finite() => p,
                        _ => {
                            debug!(symbol = %symbol, "market.raw missing/invalid price; skip");
                            continue;
                        }
                    };

                    let ctx = symbol_context_from_wallet(&wallet, &symbol);
                    let action = strategy.evaluate(&evt, &wallet, &ctx);

                    match action {
                        TradeAction::OpenLong { usdt_margin, leverage } => {
                            wallet.execute_open_long(&symbol, price, usdt_margin, leverage);
                            wallet.update_pnl();
                            tick_count += 1;
                            if tick_count % 100 == 0 {
                                wallet.log_status();
                            }
                        }
                        TradeAction::OpenShort { usdt_margin, leverage } => {
                            wallet.execute_open_short(&symbol, price, usdt_margin, leverage);
                            wallet.update_pnl();
                            tick_count += 1;
                            if tick_count % 100 == 0 {
                                wallet.log_status();
                            }
                        }
                        TradeAction::ClosePosition => {
                            wallet.execute_close(&symbol, price);
                            wallet.update_pnl();
                            tick_count += 1;
                            if tick_count % 100 == 0 {
                                wallet.log_status();
                            }
                        }
                        TradeAction::Hold => {}
                    }
                }

                msg = ctx_stream.next() => {
                    let Some(msg) = msg else {
                        warn!("market.context stream ended");
                        break;
                    };
                    let payload = msg.payload;
                    let evt: MarketContextEvent = match serde_json::from_slice(&payload) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(error = %e, "deserialize MarketContextEvent failed; skip");
                            continue;
                        }
                    };

                    wallet.update_context(&evt);
                    wallet.update_pnl();
                    wallet.apply_funding_if_due(now_ms());
                }
            }
        }

        warn!("subscription loop exited; reconnecting");
    }
}
