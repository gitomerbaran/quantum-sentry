//! JSONL file replay source for tests and dev (no ClickHouse required).

use super::{ReplayEvent, ReplaySource};
use std::path::Path;

/// File-based replay source: read ReplayEvents from JSONL, sort by ts_exchange.
#[derive(Debug, Clone)]
pub struct JsonlReplaySource {
    pub path: std::path::PathBuf,
}

impl JsonlReplaySource {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }
}

#[async_trait::async_trait]
impl ReplaySource for JsonlReplaySource {
    async fn fetch_events(&self) -> anyhow::Result<Vec<ReplayEvent>> {
        use crate::golden::GoldenBbo;
        use common::BboTick;

        let content = tokio::fs::read_to_string(&self.path).await?;
        let mut events = Vec::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            // Try to parse as ReplayEvent first (legacy format)
            if let Ok(event) = serde_json::from_str::<ReplayEvent>(line) {
                events.push(event);
            } else {
                // Try to parse as GoldenBbo (upgraded format with seq)
                if let Ok(golden) = serde_json::from_str::<GoldenBbo>(line) {
                    events.push(ReplayEvent::Bbo(golden.tick));
                } else {
                    // Try to parse as plain BboTick (fallback)
                    if let Ok(tick) = serde_json::from_str::<BboTick>(line) {
                        events.push(ReplayEvent::Bbo(tick));
                    } else {
                        anyhow::bail!(
                            "Failed to parse line as ReplayEvent, GoldenBbo, or BboTick: {}",
                            line
                        );
                    }
                }
            }
        }
        events.sort();
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn jsonl_source_sorts_by_ts_exchange() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let lines = [
            r#"{"Bbo":{"ts_exchange":1000,"ts_ingest":1005,"update_id":2,"symbol":"BTCUSDT","bid_price":50000.0,"bid_qty":1.0,"ask_price":50001.0,"ask_qty":1.0}}"#,
            r#"{"Bbo":{"ts_exchange":999,"ts_ingest":1004,"update_id":1,"symbol":"BTCUSDT","bid_price":49999.0,"bid_qty":1.0,"ask_price":50000.0,"ask_qty":1.0}}"#,
        ];
        std::fs::write(&path, lines.join("\n")).unwrap();
        let src = JsonlReplaySource::new(&path);
        let events = src.fetch_events().await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].ts_exchange(), 999);
        assert_eq!(events[1].ts_exchange(), 1000);
    }
}
