//! Minimal YAML config loader for Quantum Sentinel v2 (symbols, risk, features).

use serde::Deserialize;
use std::path::Path;

/// Symbols list (configs/symbols.yml).
#[derive(Debug, Clone, Deserialize)]
pub struct SymbolsConfig {
    pub symbols: Vec<String>,
}

/// Risk limits (configs/risk.yml).
#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    pub execution_enabled: bool,
    pub latency_max_ms: u32,
    pub spread_max_pct: f32,
    pub max_daily_loss_pct: f32,
    pub max_risk_per_trade_pct: f32,
    pub max_leverage: u32,
    /// Executor: min confidence to allow trade (default 0.55).
    #[serde(default = "default_confidence_min")]
    pub confidence_min: f32,
    /// Executor: cooldown between trades in ms (default 5000).
    #[serde(default = "default_cooldown_ms")]
    pub cooldown_ms: u64,
    /// Executor: hysteresis to enter LONG/SHORT (default 0.62).
    #[serde(default = "default_hysteresis_enter")]
    pub hysteresis_enter: f32,
    /// Executor: hysteresis to exit to NO_TRADE (default 0.58).
    #[serde(default = "default_hysteresis_exit")]
    pub hysteresis_exit: f32,
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

/// Single feature entry (configs/features.yml).
#[derive(Debug, Clone, Deserialize)]
pub struct FeatureEntry {
    pub name: String,
    pub dtype: String,
    pub window: String,
    pub missing_policy: String,
}

/// Features config (configs/features.yml).
#[derive(Debug, Clone, Deserialize)]
pub struct FeaturesConfig {
    pub version: u32,
    pub features: Vec<FeatureEntry>,
}

/// Load a YAML file from path into `T`.
pub fn load_yaml<T>(path: &Path) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    T: for<'de> Deserialize<'de>,
{
    let s = std::fs::read_to_string(path)?;
    let out = serde_yaml::from_str(&s)?;
    Ok(out)
}

/// Load symbols config from path (e.g. `configs/symbols.yml`).
pub fn load_symbols(
    path: &Path,
) -> Result<SymbolsConfig, Box<dyn std::error::Error + Send + Sync>> {
    load_yaml(path)
}

/// Load risk config from path (e.g. `configs/risk.yml`).
pub fn load_risk(path: &Path) -> Result<RiskConfig, Box<dyn std::error::Error + Send + Sync>> {
    load_yaml(path)
}

/// Load features config from path (e.g. `configs/features.yml`).
pub fn load_features(
    path: &Path,
) -> Result<FeaturesConfig, Box<dyn std::error::Error + Send + Sync>> {
    load_yaml(path)
}
