//! Async batching runtime: receive LogEvent via mpsc, buffer, flush on size or interval.

use common::LogEvent;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, warn};

use crate::config::LoggerConfig;
use crate::sink::{LogSink, LoggerCounters};

/// Run the logger task: receive events, buffer, flush when full or interval.
/// On channel full and drop_on_full, callers use try_send_log which drops and increments counter.
pub async fn run_logger<S: LogSink + 'static>(
    mut rx: mpsc::Receiver<LogEvent>,
    config: LoggerConfig,
    sink: Arc<S>,
    counters: Arc<LoggerCounters>,
) {
    if !config.enabled {
        debug!("logger disabled; draining channel");
        while rx.recv().await.is_some() {
            counters.inc_dropped();
        }
        return;
    }

    let mut buffer: Vec<LogEvent> = Vec::with_capacity(config.max_batch_size.min(1024));
    let flush_interval = Duration::from_millis(config.flush_interval_ms);
    let mut ticker = interval(flush_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(ev) => {
                        counters.inc_received();
                        buffer.push(ev);
                        if buffer.len() >= config.max_batch_size {
                            if let Err(e) = flush_with_retry(sink.as_ref(), &mut buffer, &config, &counters).await {
                                error!(error = %e, "flush failed after retries; dropping batch");
                                counters.inc_flush_errors();
                            }
                        }
                    }
                    None => {
                        debug!("logger channel closed; flushing remaining");
                        if !buffer.is_empty() {
                            let _ = flush_with_retry(sink.as_ref(), &mut buffer, &config, &counters).await;
                        }
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                if !buffer.is_empty() {
                    if let Err(e) = flush_with_retry(sink.as_ref(), &mut buffer, &config, &counters).await {
                        error!(error = %e, "flush failed after retries; dropping batch");
                        counters.inc_flush_errors();
                    }
                }
            }
        }
    }
}

async fn flush_with_retry<S: LogSink>(
    sink: &S,
    buffer: &mut Vec<LogEvent>,
    config: &LoggerConfig,
    counters: &LoggerCounters,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let batch = std::mem::take(buffer);
    let send_retries = config
        .clickhouse
        .as_ref()
        .map(|c| c.send_retries)
        .unwrap_or(3);
    let retry_backoff_ms = config
        .clickhouse
        .as_ref()
        .map(|c| c.retry_backoff_ms)
        .unwrap_or(200);

    for attempt in 0..=send_retries {
        match sink.write_batch(&batch).await {
            Ok(()) => {
                counters.inc_flushed();
                return Ok(());
            }
            Err(e) => {
                if attempt < send_retries {
                    warn!(
                        attempt = attempt + 1,
                        retries = send_retries,
                        backoff_ms = retry_backoff_ms,
                        error = %e,
                        "flush retry"
                    );
                    tokio::time::sleep(Duration::from_millis(retry_backoff_ms)).await;
                } else {
                    return Err(e);
                }
            }
        }
    }
    Ok(())
}

/// Try to send event to logger; if channel full, drop and increment dropped counter (hot path must not block).
/// Returns true if sent or dropped, false if channel closed.
pub fn try_send_log(
    tx: &mpsc::Sender<LogEvent>,
    ev: LogEvent,
    _drop_on_full: bool,
    counters: &LoggerCounters,
) -> bool {
    match tx.try_send(ev) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_ev)) => {
            counters.inc_dropped();
            true
        }
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::{LoggerCounters, MockSink};
    use common::{Action, Decision, FeatureSnapshot};
    use std::sync::atomic::Ordering;
    use std::time::Duration as StdDuration;

    #[tokio::test]
    async fn flush_triggers_by_size() {
        let config = LoggerConfig {
            enabled: true,
            flush_interval_ms: 10_000,
            max_batch_size: 5,
            drop_on_full: true,
            log_raw_ticks: true,
            clickhouse: None,
        };
        let sink = Arc::new(MockSink::new());
        let counters = Arc::new(LoggerCounters::new());
        let (tx, rx) = mpsc::channel(100);
        let sink_clone = Arc::clone(&sink);
        let counters_clone = Arc::clone(&counters);
        let config_clone = config.clone();
        let handle: tokio::task::JoinHandle<()> = tokio::spawn(async move {
            run_logger(rx, config_clone, sink_clone, counters_clone).await;
        });
        for i in 0..5 {
            let snap = FeatureSnapshot {
                ts_exchange: 1000 + i,
                symbol: "BTCUSDT".to_string(),
                features: vec![0.1, 0.0],
                ready: true,
                data_latency_ms: 50,
                spread_pct: 0.01,
                feature_version: 1,
            };
            tx.send(LogEvent::FeatureSnapshot {
                ts_ingest: 2000,
                snap,
            })
            .await
            .unwrap();
        }
        drop(tx);
        tokio::time::timeout(StdDuration::from_secs(2), handle)
            .await
            .unwrap()
            .unwrap();
        let batches = sink.batches.lock().unwrap();
        assert!(!batches.is_empty(), "at least one batch flushed by size");
        assert!(batches[0].len() == 5);
    }

    #[tokio::test]
    async fn drop_on_full_increments_dropped() {
        // tokio mpsc requires capacity > 0; use cap 1 then fill it so second try_send gets Full
        let (tx, mut _rx) = mpsc::channel(1);
        let counters = LoggerCounters::new();
        let ev1 = LogEvent::Decision {
            ts_ingest: 1000,
            dec: Decision {
                ts_exchange: 1000,
                symbol: "BTCUSDT".to_string(),
                action: Action::NoTrade,
                confidence: 0.0,
                reason_codes: vec!["test".to_string()],
                model_version: "v1".to_string(),
            },
        };
        let ev2 = LogEvent::Decision {
            ts_ingest: 1001,
            dec: Decision {
                ts_exchange: 1001,
                symbol: "ETHUSDT".to_string(),
                action: Action::NoTrade,
                confidence: 0.0,
                reason_codes: vec!["test2".to_string()],
                model_version: "v1".to_string(),
            },
        };
        tx.try_send(ev1).unwrap(); // fill channel
        try_send_log(&tx, ev2, true, &counters); // this will hit Full and increment dropped
        assert_eq!(counters.events_dropped.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn flush_triggers_by_timer() {
        let config = LoggerConfig {
            enabled: true,
            flush_interval_ms: 50,
            max_batch_size: 1000,
            drop_on_full: true,
            log_raw_ticks: true,
            clickhouse: None,
        };
        let sink = Arc::new(MockSink::new());
        let counters = Arc::new(LoggerCounters::new());
        let (tx, rx) = mpsc::channel(100);
        let handle: tokio::task::JoinHandle<()> = tokio::spawn(run_logger(
            rx,
            config,
            Arc::clone(&sink),
            Arc::clone(&counters),
        ));
        tx.send(LogEvent::FeatureSnapshot {
            ts_ingest: 1000,
            snap: FeatureSnapshot {
                ts_exchange: 1000,
                symbol: "BTCUSDT".to_string(),
                features: vec![0.1],
                ready: true,
                data_latency_ms: 0,
                spread_pct: 0.01,
                feature_version: 1,
            },
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(tx);
        let _ = tokio::time::timeout(StdDuration::from_secs(2), handle)
            .await
            .unwrap();
        let batches = sink.batches.lock().unwrap();
        assert!(!batches.is_empty(), "batch flushed by timer");
    }

    #[tokio::test]
    async fn decision_reason_codes_in_mock_sink() {
        let sink = MockSink::new();
        let reasons = vec!["low_confidence".to_string(), "meta_mismatch".to_string()];
        let dec = Decision {
            ts_exchange: 1000,
            symbol: "BTCUSDT".to_string(),
            action: Action::Long,
            confidence: 0.7,
            reason_codes: reasons.clone(),
            model_version: "v1-mock".to_string(),
        };
        let batch = vec![LogEvent::Decision {
            ts_ingest: 2000,
            dec: dec.clone(),
        }];
        sink.write_batch(&batch).await.unwrap();
        let batches = sink.batches.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 1);
        match &batches[0][0] {
            LogEvent::Decision { dec: d, .. } => {
                assert_eq!(d.reason_codes, reasons);
            }
            _ => panic!("expected Decision"),
        }
    }
}
