//! Executor config: gates + stabilization. Load from configs/risk.yml or env fallback.

use common::RiskConfig;
use serde::Deserialize;
use std::path::Path;

/// Executor-specific config (subset of risk.yml used by executor).
#[derive(Clone, Debug)]
pub struct ExecutorConfig {
    pub execution_enabled: bool,
    pub latency_max_ms: u32,
    pub spread_max_pct: f32,
    pub confidence_min: f32,
    pub cooldown_ms: u64,
    pub hysteresis_enter: f32,
    pub hysteresis_exit: f32,
    pub allow_short: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct ExecutorConfigFile {
    #[serde(default = "default_execution_enabled")]
    pub execution_enabled: bool,
    #[serde(default = "default_latency_max_ms", alias = "max_latency_ms")]
    pub latency_max_ms: u32,
    #[serde(default = "default_spread_max_pct", alias = "max_spread_pct")]
    pub spread_max_pct: f32,
    #[serde(default = "default_confidence_min", alias = "min_confidence")]
    pub confidence_min: f32,
    #[serde(default = "default_cooldown_ms")]
    pub cooldown_ms: u64,
    #[serde(default = "default_hysteresis_enter")]
    pub hysteresis_enter: f32,
    #[serde(default = "default_hysteresis_exit")]
    pub hysteresis_exit: f32,
    #[serde(default = "default_allow_short")]
    pub allow_short: bool,
}

fn default_execution_enabled() -> bool {
    false
}
fn default_latency_max_ms() -> u32 {
    200
}
fn default_spread_max_pct() -> f32 {
    0.20
}
fn default_confidence_min() -> f32 {
    0.55
}
fn default_cooldown_ms() -> u64 {
    5000
}
fn default_hysteresis_enter() -> f32 {
    0.62
}
fn default_hysteresis_exit() -> f32 {
    0.58
}
fn default_allow_short() -> bool {
    true
}

impl ExecutorConfig {
    /// Load from risk.yml; use env overrides if set.
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let risk: RiskConfig = common::load_yaml(path)?;
        Ok(Self::from_risk(&risk))
    }

    /// Load from an executor-specific tuning YAML (see configs/executor_*.yml).
    /// This is intended for replay/paper runs where we want to tune gates without touching risk.yml.
    pub fn load_executor_config(
        path: &Path,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let cfg: ExecutorConfigFile = common::load_yaml(path)?;
        Ok(Self {
            execution_enabled: cfg.execution_enabled,
            latency_max_ms: cfg.latency_max_ms,
            spread_max_pct: cfg.spread_max_pct,
            confidence_min: cfg.confidence_min,
            cooldown_ms: cfg.cooldown_ms,
            hysteresis_enter: cfg.hysteresis_enter,
            hysteresis_exit: cfg.hysteresis_exit,
            allow_short: cfg.allow_short,
        })
    }

    /// Build from RiskConfig (risk.yml).
    pub fn from_risk(r: &RiskConfig) -> Self {
        Self {
            execution_enabled: r.execution_enabled,
            latency_max_ms: r.latency_max_ms,
            spread_max_pct: r.spread_max_pct,
            confidence_min: r.confidence_min,
            cooldown_ms: r.cooldown_ms,
            hysteresis_enter: r.hysteresis_enter,
            hysteresis_exit: r.hysteresis_exit,
            allow_short: true,
        }
    }

    /// Default config (env overrides when available).
    pub fn from_env_or_default() -> Self {
        let execution_enabled = std::env::var("EXECUTION_ENABLED")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(false);
        let latency_max_ms = std::env::var("LATENCY_MAX_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(200);
        let spread_max_pct = std::env::var("SPREAD_MAX_PCT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.20);
        let confidence_min = std::env::var("CONFIDENCE_MIN")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.55);
        let cooldown_ms = std::env::var("COOLDOWN_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5000);
        let hysteresis_enter = std::env::var("HYSTERESIS_ENTER")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.62);
        let hysteresis_exit = std::env::var("HYSTERESIS_EXIT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.58);
        let allow_short = std::env::var("EXECUTOR_ALLOW_SHORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(true);
        Self {
            execution_enabled,
            latency_max_ms,
            spread_max_pct,
            confidence_min,
            cooldown_ms,
            hysteresis_enter,
            hysteresis_exit,
            allow_short,
        }
    }
}
