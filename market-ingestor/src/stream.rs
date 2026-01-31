use anyhow::{Context, Result};
use bytes::Bytes;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};
use url::Url;

use crate::messaging::NatsPublisher;
use crate::model::{CombinedStream, DepthUpdateEvent, TradeEvent};

const BINANCE_WS_BASE: &str = "wss://stream.binance.com:9443/stream?streams=";

/// Manages a combined Binance WS connection for a dynamic symbol universe.
///
/// Responsibilities:
/// - Build combined stream URL from the symbol list.
/// - Connect and read messages.
/// - Reconnect on:
///   a) Universe update (hot swap) via watch channel.
///   b) Connection drop / read error, with exponential backoff.
///
/// NOTE (Hot Path):
/// The message loop is on the ingestion hot path. We keep it simple:
/// - Minimal parsing to decide event type
/// - Avoid heavy logging per message (log compact summaries)
/// - Use `tokio::select!` to react immediately to universe updates and reconnect
pub struct StreamManager {
    max_backoff: Duration,
    nats: Arc<NatsPublisher>,
}

impl StreamManager {
    pub fn new(nats: Arc<NatsPublisher>) -> Self {
        Self {
            max_backoff: Duration::from_secs(30),
            nats,
        }
    }

    fn build_url(symbols: &[String]) -> Result<Url> {
        if symbols.is_empty() {
            anyhow::bail!("cannot build WS URL: empty symbol list");
        }

        // For each symbol, subscribe to both trade and depth streams.
        // Using @depth@100ms for frequent order book deltas.
        let mut streams: Vec<String> = Vec::with_capacity(symbols.len() * 2);
        for sym in symbols {
            streams.push(format!("{sym}@trade"));
            streams.push(format!("{sym}@depth@100ms"));
        }

        let stream_path = streams.join("/"); // combined stream separator
        let full = format!("{BINANCE_WS_BASE}{stream_path}");

        Url::parse(&full).with_context(|| format!("failed to parse ws url: {full}"))
    }

    pub async fn run(self, mut rx: watch::Receiver<Vec<String>>) -> Result<()> {
        // Wait for the first non-empty universe.
        loop {
            let current = rx.borrow().clone();
            if !current.is_empty() {
                break;
            }
            info!("universe is empty at startup; waiting for first universe update...");
            if rx.changed().await.is_err() {
                info!("universe sender dropped; shutting down stream manager");
                return Ok(());
            }
        }

        let mut backoff = Duration::from_millis(500);

        loop {
            let symbols = rx.borrow().clone();
            if symbols.is_empty() {
                // If universe becomes empty, just wait.
                warn!("universe is empty; waiting for update...");
                if rx.changed().await.is_err() {
                    info!("universe sender dropped; shutting down stream manager");
                    return Ok(());
                }
                continue;
            }

            let url = Self::build_url(&symbols)?;
            info!(
                symbols = symbols.len(),
                url = %url,
                "connecting to binance ws (combined stream)"
            );

            let connect_result = tokio_tungstenite::connect_async(url.as_str()).await;

            let (ws_stream, _resp) = match connect_result {
                Ok(ok) => {
                    backoff = Duration::from_millis(500); // reset backoff after a successful connect
                    info!("ws connected, receiving trade and depth stream (use RUST_LOG=debug for per-message logs)");
                    ok
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        backoff_ms = backoff.as_millis(),
                        "ws connect failed; backing off and retrying"
                    );
                    sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, self.max_backoff);
                    continue;
                }
            };

            // Split into write/read so we can respond to Ping frames.
            let (mut write, mut read) = ws_stream.split();

            // Read loop: break on universe update or socket failure.
            // HOT SWAP: rx.changed() triggers a graceful break, then outer loop reconnects.
            loop {
                tokio::select! {
                    changed = rx.changed() => {
                        match changed {
                            Ok(()) => {
                                let new_symbols = rx.borrow().clone();
                                if new_symbols != symbols {
                                    info!(
                                        old = symbols.len(),
                                        new = new_symbols.len(),
                                        "universe updated -> reconnecting websocket (hot swap)"
                                    );
                                    break; // reconnect using new universe
                                } else {
                                    // Sometimes watch can trigger even if content is the same; keep going.
                                    debug!("universe watch changed but symbols identical; continuing without reconnect");
                                }
                            }
                            Err(_) => {
                                info!("universe sender dropped; shutting down stream manager");
                                return Ok(());
                            }
                        }
                    }

                    msg = read.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                if let Err(e) = self.handle_text_message(&text).await {
                                    // Parsing errors should not crash the stream; warn and continue.
                                    warn!(error = %e, "failed to handle text message");
                                }
                            }
                            Some(Ok(Message::Binary(_bin))) => {
                                // Binance typically sends text; ignore binary to keep hot path simple.
                                debug!("received binary message (ignored)");
                            }
                            Some(Ok(Message::Frame(_))) => {
                                // Low-level frame; ignore (we handle Text/Ping/Pong/Close explicitly).
                            }
                            Some(Ok(Message::Ping(payload))) => {
                                // Respond with Pong to keep connection healthy.
                                if let Err(e) = write.send(Message::Pong(payload)).await {
                                    warn!(error = %e, "failed to send pong; will reconnect");
                                    break;
                                }
                            }
                            Some(Ok(Message::Pong(_))) => {
                                // Optional: track heartbeat latency here.
                            }
                            Some(Ok(Message::Close(frame))) => {
                                warn!(?frame, "ws closed by server; reconnecting");
                                break;
                            }
                            Some(Err(e)) => {
                                warn!(error = %e, "ws read error; reconnecting");
                                break;
                            }
                            None => {
                                warn!("ws stream ended; reconnecting");
                                break;
                            }
                        }
                    }
                }
            }

            // If we got here, we intentionally reconnect (either universe hot swap or ws error).
            // Add a small sleep to avoid tight reconnect loops on transient failures.
            sleep(Duration::from_millis(200)).await;
        }
    }

    async fn handle_text_message(&self, text: &str) -> Result<()> {
        // HOT PATH NOTE:
        // We parse into Value first to cheaply inspect `data.e` (event type),
        // then parse into the strongly typed struct. This is a trade-off:
        // - Simple, robust routing
        // - Slight overhead of a second parse when we re-hydrate typed structs
        //
        // In production, consider using `serde_json::value::RawValue` to avoid double parsing.

        let envelope: CombinedStream<Value> = serde_json::from_str(text)
            .context("failed to parse message as CombinedStream<Value>")?;

        let event_type = match envelope.data.get("e").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => "unknown",
        };

        match event_type {
            "trade" => {
                let typed: CombinedStream<TradeEvent> =
                    serde_json::from_str(text).map_err(|e| {
                        let snippet: String = text.chars().take(250).collect();
                        anyhow::anyhow!("trade parse: {} | snippet: {:?}", e, snippet)
                    })?;
                debug!(
                    stream = %typed.stream,
                    sym = %typed.data.symbol,
                    price = %typed.data.price,
                    qty = %typed.data.quantity,
                    maker = typed.data.is_buyer_maker,
                    "trade"
                );

                let payload = serde_json::json!({
                    "stream": typed.stream,
                    "event_type": "trade",
                    "ts_ingest": Utc::now().to_rfc3339(),
                    "data": typed.data
                });
                if let Ok(bytes) = serde_json::to_vec(&payload) {
                    if let Err(e) = self.nats.publish(Bytes::from(bytes)).await {
                        warn!(error = %e, "nats publish trade");
                    }
                } else {
                    warn!("serialize trade payload failed");
                }
            }
            "depthUpdate" => {
                let typed: CombinedStream<DepthUpdateEvent> =
                    serde_json::from_str(text).context("failed to parse depth update event")?;
                debug!(
                    stream = %typed.stream,
                    sym = %typed.data.symbol,
                    bids = typed.data.bids.len(),
                    asks = typed.data.asks.len(),
                    "depth"
                );

                let payload = serde_json::json!({
                    "stream": typed.stream,
                    "event_type": "depthUpdate",
                    "ts_ingest": Utc::now().to_rfc3339(),
                    "data": typed.data
                });
                if let Ok(bytes) = serde_json::to_vec(&payload) {
                    if let Err(e) = self.nats.publish(Bytes::from(bytes)).await {
                        warn!(error = %e, "nats publish depthUpdate");
                    }
                } else {
                    warn!("serialize depthUpdate payload failed");
                }
            }
            other => {
                debug!(
                    event_type = other,
                    stream = %envelope.stream,
                    "unsupported event type"
                );

                let payload = serde_json::json!({
                    "stream": envelope.stream,
                    "event_type": other,
                    "ts_ingest": Utc::now().to_rfc3339(),
                    "data": envelope.data
                });
                if let Ok(bytes) = serde_json::to_vec(&payload) {
                    if let Err(e) = self.nats.publish(Bytes::from(bytes)).await {
                        warn!(error = %e, "nats publish unknown event");
                    }
                } else {
                    warn!("serialize unknown event payload failed");
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_url_contains_streams() {
        let symbols = vec!["btcusdt".to_string(), "ethusdt".to_string()];
        let url = StreamManager::build_url(&symbols).expect("url should build in test");
        let s = url.as_str();
        assert!(s.contains("btcusdt@trade"));
        assert!(s.contains("btcusdt@depth@100ms"));
        assert!(s.contains("ethusdt@trade"));
        assert!(s.contains("ethusdt@depth@100ms"));
    }
}
