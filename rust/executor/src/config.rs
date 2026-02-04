//! Executor config: gates + stabilization. Load from configs/risk.yml or env fallback.

use common::RiskConfig;
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
}

impl ExecutorConfig {
    /// Load from risk.yml; use env overrides if set.
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let risk: RiskConfig = common::load_yaml(path)?;
        Ok(Self::from_risk(&risk))
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
        Self {
            execution_enabled,
            latency_max_ms,
            spread_max_pct,
            confidence_min,
            cooldown_ms,
            hysteresis_enter,
            hysteresis_exit,
        }
    }
}
