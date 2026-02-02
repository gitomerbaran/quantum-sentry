use anyhow::Result;
use bytes::Bytes;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{info, warn};

/// Best-effort NATS publisher with lazy connect + reconnect.
/// This is intentionally simple and resilient:
/// - If NATS is down, we log and keep going (single attempt per publish, no infinite loop).
/// - On publish failure, we drop the client and reconnect on next publish.
pub struct NatsPublisher {
    server: String,
    subject: String,
    inner: Mutex<Inner>,
}

struct Inner {
    client: Option<async_nats::Client>,
    backoff: Duration,
    max_backoff: Duration,
}

impl NatsPublisher {
    pub fn new(server: String, subject: String) -> Self {
        Self {
            server,
            subject,
            inner: Mutex::new(Inner {
                client: None,
                backoff: Duration::from_millis(250),
                max_backoff: Duration::from_secs(10),
            }),
        }
    }

    /// Publish message bytes to a specific subject (e.g. market.depth.BTCUSDT).
    /// Same best-effort semantics as publish(); uses same connection.
    pub async fn publish_to(&self, subject: &str, payload: Bytes) -> Result<()> {
        {
            let inner = self.inner.lock().await;
            if inner.client.is_none() {
                drop(inner);
                self.ensure_connected().await?;
            }
        }
        let mut inner = self.inner.lock().await;
        if let Some(client) = inner.client.as_ref() {
            match client.publish(subject.to_string(), payload).await {
                Ok(_) => {
                    inner.backoff = Duration::from_millis(250);
                    return Ok(());
                }
                Err(e) => {
                    warn!(error = %e, subject = %subject, "nats publish_to failed; dropping client");
                    inner.client = None;
                    inner.backoff = std::cmp::min(inner.backoff * 2, inner.max_backoff);
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Publish message bytes to NATS subject (default subject for this publisher).
    ///
    /// Best-effort semantics:
    /// - Returns Ok even if publishing fails after logging, so ingestion hot path isn't fatal.
    /// - Uses single connect attempt per call (no infinite loop); backoff then return.
    pub async fn publish(&self, payload: Bytes) -> Result<()> {
        // If not connected, try once to connect (non-blocking for WS loop).
        {
            let inner = self.inner.lock().await;
            if inner.client.is_none() {
                drop(inner);
                self.ensure_connected().await?;
            }
        }

        // Attempt publish
        let mut inner = self.inner.lock().await;
        if let Some(client) = inner.client.as_ref() {
            match client.publish(self.subject.clone(), payload.clone()).await {
                Ok(_) => {
                    inner.backoff = Duration::from_millis(250);
                    return Ok(());
                }
                Err(e) => {
                    warn!(error = %e, "nats publish failed; dropping client and retrying later");
                    inner.client = None;
                    inner.backoff = std::cmp::min(inner.backoff * 2, inner.max_backoff);
                    return Ok(());
                }
            }
        }

        // Not connected after ensure_connected (NATS down); best-effort, don't stall.
        Ok(())
    }

    /// Single connect attempt; on failure sleep(backoff), increase backoff, return Ok(()) so WS loop doesn't stall.
    async fn ensure_connected(&self) -> Result<()> {
        let inner = self.inner.lock().await;

        if inner.client.is_some() {
            return Ok(());
        }

        let backoff = inner.backoff;
        let max_backoff = inner.max_backoff;
        let server = self.server.clone();
        drop(inner);

        match async_nats::connect(server.clone()).await {
            Ok(client) => {
                let mut inner = self.inner.lock().await;
                inner.client = Some(client);
                inner.backoff = Duration::from_millis(250);
                info!(server = %server, "connected to NATS");
                Ok(())
            }
            Err(e) => {
                warn!(
                    error = %e,
                    server = %server,
                    backoff_ms = backoff.as_millis(),
                    "failed to connect to NATS; will retry on next publish"
                );
                sleep(backoff).await;
                let mut inner = self.inner.lock().await;
                inner.client = None;
                inner.backoff = std::cmp::min(backoff * 2, max_backoff);
                Ok(())
            }
        }
    }
}
