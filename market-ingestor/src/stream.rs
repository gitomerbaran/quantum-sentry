use anyhow::{Context, Result};
use bytes::Bytes;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};
use url::Url;

use crate::messaging::NatsPublisher;
use crate::model::now_ms;
use crate::model::{
    CombinedStream, DepthSnapshot, DepthUpdateEvent, FuturesDepthEvent, FuturesForceOrderEvent,
    FuturesMarkPriceUpdate, FuturesOpenInterestUpdate, LiquidationTick, MarketContextEvent,
    TradeEvent,
};

const BINANCE_WS_BASE: &str = "wss://stream.binance.com:9443/stream?streams=";
const BINANCE_FUTURES_WS_BASE: &str = "wss://fstream.binance.com/stream?streams=";
/// Binance USDT-M futures REST: GET /fapi/v1/openInterest?symbol=XXX
const BINANCE_FUTURES_OPEN_INTEREST_URL: &str = "https://fapi.binance.com/fapi/v1/openInterest";
/// Binance Futures server time (for clock skew correction: ts_ingest in Binance time base).
const BINANCE_FUTURES_TIME_URL: &str = "https://fapi.binance.com/fapi/v1/time";

#[derive(Debug, Deserialize)]
struct BinanceTimeResponse {
    #[serde(rename = "serverTime")]
    server_time: i64,
}

/// Fetches Binance Futures server time every 30s and updates clock_offset_ms (local_now - server_time).
/// ts_ingest_normalized = now_ms() - offset gives ingest time in Binance time base so latency metrics are correct.
pub async fn run_clock_sync(client: Client, clock_offset_ms: Arc<AtomicI64>) -> Result<()> {
    // Initial sync with retries so offset is set before depth/context messages (avoids ~1–6s false latency).
    const INITIAL_RETRIES: u32 = 5;
    const INITIAL_DELAY_MS: u64 = 2000;
    for attempt in 1..=INITIAL_RETRIES {
        let local_before = crate::model::now_ms();
        if let Ok(resp) = client.get(BINANCE_FUTURES_TIME_URL).send().await {
            if resp.status().is_success() {
                if let Ok(t) = resp.json::<BinanceTimeResponse>().await {
                    let local_after = crate::model::now_ms();
                    let rtt_half = (local_after - local_before) / 2;
                    let local_mid = local_before + rtt_half;
                    let offset = local_mid - t.server_time;
                    clock_offset_ms.store(offset, Ordering::Relaxed);
                    info!(offset_ms = offset, attempt, "clock sync: initial Binance offset set");
                    break;
                }
            }
        }
        if attempt < INITIAL_RETRIES {
            warn!(attempt, "clock sync: initial fetch failed; retry in {}ms", INITIAL_DELAY_MS);
            sleep(Duration::from_millis(INITIAL_DELAY_MS)).await;
        } else {
            warn!("clock sync: initial fetch failed after {} attempts; latency metrics may show clock skew until next sync", INITIAL_RETRIES);
        }
    }

    let mut interval = tokio::time::interval(Duration::from_secs(30));
    interval.tick().await; // first tick immediate
    loop {
        interval.tick().await;
        let local_before = crate::model::now_ms();
        match client.get(BINANCE_FUTURES_TIME_URL).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    warn!("clock sync: Binance time API non-OK status");
                    continue;
                }
                match resp.json::<BinanceTimeResponse>().await {
                    Ok(t) => {
                        let local_after = crate::model::now_ms();
                        let rtt_half = (local_after - local_before) / 2;
                        let local_mid = local_before + rtt_half;
                        let offset = local_mid - t.server_time;
                        clock_offset_ms.store(offset, Ordering::Relaxed);
                        info!(offset_ms = offset, "clock sync: Binance offset updated");
                    }
                    Err(e) => {
                        warn!(error = %e, "clock sync: Binance time parse failed");
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "clock sync: Binance time fetch failed");
            }
        }
    }
}

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

// ---------- Futures context stream (mark price, funding rate, open interest) ----------

/// REST response for GET /fapi/v1/openInterest?symbol=XXX
#[derive(Debug, Deserialize)]
struct OpenInterestRestResponse {
    #[serde(rename = "openInterest")]
    open_interest: String,
    symbol: String,
}

/// Fetches open interest from Binance REST and updates the shared cache every 60s.
/// USDT-M futures WS does not reliably provide openInterest stream; REST fills the cache.
pub async fn run_oi_fetcher(
    client: Client,
    oi_cache: Arc<Mutex<HashMap<String, f64>>>,
    mut rx: watch::Receiver<Vec<String>>,
) -> Result<()> {
    loop {
        let symbols = rx.borrow().clone();
        if symbols.is_empty() {
            if rx.changed().await.is_err() {
                return Ok(());
            }
            continue;
        }
        let mut updated = 0u32;
        for sym in &symbols {
            let symbol = sym.to_uppercase();
            let url = format!("{}?symbol={}", BINANCE_FUTURES_OPEN_INTEREST_URL, symbol);
            match client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    if let Ok(rest) = resp.json::<OpenInterestRestResponse>().await {
                        if let Some(oi) = crate::model::parse_f64(&rest.open_interest) {
                            oi_cache.lock().await.insert(rest.symbol.to_uppercase(), oi);
                            updated += 1;
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    debug!(symbol = %symbol, error = %e, "OI REST fetch failed");
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if updated > 0 {
            info!(symbols = updated, "OI cache updated from REST");
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

/// Manages Binance Futures WS for context (mark price @1s). Publishes to NATS `market.context`.
/// Stateful: uses shared OI cache (filled by REST fetcher or WS openInterest if available).
/// Uses clock_offset_ms (local - Binance server time) so ts_ingest is in Binance time base for correct latency.
pub struct FuturesContextStreamManager {
    max_backoff: Duration,
    nats: Arc<NatsPublisher>,
    oi_cache: Arc<Mutex<HashMap<String, f64>>>,
    clock_offset_ms: Arc<AtomicI64>,
}

impl FuturesContextStreamManager {
    pub fn new(
        nats: Arc<NatsPublisher>,
        oi_cache: Arc<Mutex<HashMap<String, f64>>>,
        clock_offset_ms: Arc<AtomicI64>,
    ) -> Self {
        Self {
            max_backoff: Duration::from_secs(30),
            nats,
            oi_cache,
            clock_offset_ms,
        }
    }

    fn build_ws_url(symbols: &[String]) -> Result<Url> {
        if symbols.is_empty() {
            anyhow::bail!("cannot build Futures WS URL: empty symbol list");
        }
        let mut streams: Vec<String> = Vec::with_capacity(symbols.len() * 4);
        for sym in symbols {
            let s = sym.to_lowercase();
            streams.push(format!("{s}@markPrice@1s"));
            streams.push(format!("{s}@openInterest@1s"));
            streams.push(format!("{s}@depth5@100ms"));
            streams.push(format!("{s}@forceOrder"));
        }
        let stream_path = streams.join("/");
        let full = format!("{BINANCE_FUTURES_WS_BASE}{stream_path}");
        Url::parse(&full).with_context(|| format!("failed to parse futures ws url: {full}"))
    }

    pub async fn run(self, mut rx: watch::Receiver<Vec<String>>) -> Result<()> {
        loop {
            let current = rx.borrow().clone();
            if !current.is_empty() {
                break;
            }
            info!("futures context: universe empty at startup; waiting for first update...");
            if rx.changed().await.is_err() {
                info!("universe sender dropped; shutting down futures context manager");
                return Ok(());
            }
        }

        let mut backoff = Duration::from_millis(500);

        loop {
            let symbols = rx.borrow().clone();
            if symbols.is_empty() {
                warn!("futures context: universe empty; waiting for update...");
                if rx.changed().await.is_err() {
                    return Ok(());
                }
                continue;
            }

            let url = Self::build_ws_url(&symbols)?;
            info!(
                symbols = symbols.len(),
                url = %url,
                "futures context: connecting to Binance Futures WS"
            );

            let connect_result = tokio_tungstenite::connect_async(url.as_str()).await;

            let (ws_stream, _resp) = match connect_result {
                Ok(ok) => {
                    backoff = Duration::from_millis(500);
                    info!("futures context: WS connected (markPrice + openInterest + depth5 + forceOrder)");
                    ok
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        backoff_ms = backoff.as_millis(),
                        "futures context: WS connect failed; backing off"
                    );
                    sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, self.max_backoff);
                    continue;
                }
            };

            let (mut write, mut read) = ws_stream.split();

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
                                        "futures context: universe updated -> reconnecting (hot swap)"
                                    );
                                    break;
                                }
                            }
                            Err(_) => {
                                info!("universe sender dropped; shutting down futures context manager");
                                return Ok(());
                            }
                        }
                    }

                    msg = read.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                if let Err(e) = self.handle_futures_message(&text).await {
                                    warn!(error = %e, "futures context: failed to handle message");
                                }
                            }
                            Some(Ok(Message::Ping(payload))) => {
                                if let Err(e) = write.send(Message::Pong(payload)).await {
                                    warn!(error = %e, "futures context: failed to send pong");
                                    break;
                                }
                            }
                            Some(Ok(Message::Close(frame))) => {
                                warn!(?frame, "futures context: WS closed by server");
                                break;
                            }
                            Some(Err(e)) => {
                                warn!(error = %e, "futures context: WS read error");
                                break;
                            }
                            None => {
                                warn!("futures context: WS stream ended");
                                break;
                            }
                            _ => {}
                        }
                    }
                }
            }

            sleep(Duration::from_millis(200)).await;
        }
    }

    /// Hot path: capture ts_ingest in Binance time base (now_ms - clock_offset) for correct latency metrics.
    async fn handle_futures_message(&self, text: &str) -> Result<()> {
        let ts_ingest = now_ms() - self.clock_offset_ms.load(Ordering::Relaxed);

        let envelope: CombinedStream<Value> = serde_json::from_str(text)
            .context("futures context: parse as CombinedStream<Value>")?;

        let event_type = envelope
            .data
            .get("e")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        match event_type {
            "depthUpdate" => {
                let typed: CombinedStream<FuturesDepthEvent> =
                    serde_json::from_str(text).context("futures context: parse depthUpdate")?;
                let snap = DepthSnapshot::from_depth5(typed.data, ts_ingest);
                let subj = format!("market.depth.{}", snap.symbol);
                match serde_json::to_vec(&snap) {
                    Ok(bytes) => {
                        let payload = Bytes::from(bytes);
                        if let Err(e) = self.nats.publish_to(&subj, payload).await {
                            warn!(error = %e, subject = %subj, "futures context: NATS publish depth failed");
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "futures context: serialize depth snapshot failed");
                    }
                }
            }
            "forceOrder" => {
                let typed: CombinedStream<FuturesForceOrderEvent> =
                    serde_json::from_str(text).context("futures context: parse forceOrder")?;
                if let Some(liq) = LiquidationTick::from_force_order(typed.data, ts_ingest) {
                    let subj = format!("market.liq.{}", liq.symbol);
                    match serde_json::to_vec(&liq) {
                        Ok(bytes) => {
                            let payload = Bytes::from(bytes);
                            if let Err(e) = self.nats.publish_to(&subj, payload).await {
                                warn!(error = %e, subject = %subj, "futures context: NATS publish liq failed");
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "futures context: serialize liquidation failed");
                        }
                    }
                } else {
                    warn!("futures context: failed to normalize liquidation payload");
                }
            }
            "markPriceUpdate" => {
                let typed: CombinedStream<FuturesMarkPriceUpdate> =
                    serde_json::from_str(text).context("futures context: parse markPriceUpdate")?;
                let symbol_key = typed.data.symbol.to_uppercase();
                let cached_oi = self.oi_cache.lock().await.get(&symbol_key).copied();
                let event = MarketContextEvent::from_mark(typed.data, ts_ingest, cached_oi);
                let bytes = match serde_json::to_vec(&event) {
                    Ok(b) => Bytes::from(b),
                    Err(e) => {
                        warn!(error = %e, "futures context: serialize mark context failed");
                        return Ok(());
                    }
                };
                if let Err(e) = self.nats.publish(bytes).await {
                    warn!(error = %e, "futures context: NATS publish mark failed");
                }
            }
            "openInterest" => {
                let typed: CombinedStream<FuturesOpenInterestUpdate> =
                    serde_json::from_str(text).context("futures context: parse openInterest")?;
                let symbol_key = typed.data.symbol.to_uppercase();
                if let Some(oi) = crate::model::parse_f64(&typed.data.open_interest) {
                    self.oi_cache.lock().await.insert(symbol_key, oi);
                }
                // Do not publish OI-only to NATS; we attach cached OI to the next mark price event.
            }
            other => {
                debug!(
                    event_type = other,
                    "futures context: unsupported event type"
                );
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
