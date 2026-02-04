//! Logger config: batch size, flush interval, optional ClickHouse. Load from configs/logger.yml or env.

use serde::Deserialize;
use std::path::Path;

/// Cold-path logger config (configs/logger.yml).
#[derive(Clone, Debug, Deserialize)]
pub struct LoggerConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    #[serde(default = "default_drop_on_full")]
    pub drop_on_full: bool,
    #[serde(default = "default_log_raw_ticks")]
    pub log_raw_ticks: bool,
    pub clickhouse: Option<ClickHouseConfig>,
}

fn default_enabled() -> bool {
    true
}
fn default_flush_interval_ms() -> u64 {
    500
}
fn default_max_batch_size() -> usize {
    2000
}
fn default_drop_on_full() -> bool {
    true
}
fn default_log_raw_ticks() -> bool {
    true
}

/// ClickHouse connection and retry settings (optional, used when logger has clickhouse feature).
#[derive(Clone, Debug, Deserialize)]
pub struct ClickHouseConfig {
    #[serde(default = "default_ch_url")]
    pub url: String,
    #[serde(default = "default_ch_database")]
    pub database: String,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_send_retries")]
    pub send_retries: u32,
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
}

fn default_ch_url() -> String {
    "http://localhost:8123".to_string()
}
fn default_ch_database() -> String {
    "quantum".to_string()
}
fn default_send_retries() -> u32 {
    5
}
fn default_retry_backoff_ms() -> u64 {
    200
}

impl LoggerConfig {
    /// Load from YAML path (e.g. configs/logger.yml).
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let s = std::fs::read_to_string(path)?;
        let out: LoggerConfig = serde_yaml::from_str(&s)?;
        Ok(out)
    }

    /// From env or default: LOGGER_CONFIG path, or env vars, or defaults.
    pub fn from_env_or_default() -> Self {
        if let Ok(path) = std::env::var("LOGGER_CONFIG") {
            if let Ok(c) = Self::load(Path::new(&path)) {
                return c;
            }
        }
        Self {
            enabled: std::env::var("LOGGER_ENABLED")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(true),
            flush_interval_ms: std::env::var("LOGGER_FLUSH_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(500),
            max_batch_size: std::env::var("LOGGER_MAX_BATCH_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2000),
            drop_on_full: std::env::var("LOGGER_DROP_ON_FULL")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(true),
            log_raw_ticks: std::env::var("LOGGER_LOG_RAW_TICKS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(true),
            clickhouse: None,
        }
    }
}
