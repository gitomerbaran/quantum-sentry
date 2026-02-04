//! Model config (configs/model.yml) and model meta (models/model_meta.json).
//! YAML primary; env override optional.

use serde::Deserialize;
use std::path::Path;

/// Model config (configs/model.yml).
#[derive(Debug, Clone, Deserialize)]
pub struct ModelConfig {
    pub model_version: String,
    pub onnx_model_path: String,
    pub meta_path: String,
    pub warmup_runs: u32,
    pub expected_feature_len: usize,
    pub output_kind: String, // "binary_probs" or "logits"
}

/// Output section of model meta JSON.
#[derive(Debug, Clone, Deserialize)]
pub struct ModelMetaOutput {
    pub kind: String,
    pub names: Vec<String>,
}

/// Model meta (models/model_meta.json).
#[derive(Debug, Clone, Deserialize)]
pub struct ModelMeta {
    pub model_version: String,
    pub feature_version: u32,
    pub feature_order: Vec<String>,
    pub expected_feature_len: usize,
    pub output: ModelMetaOutput,
}

impl ModelConfig {
    /// Load from YAML path. Env overrides: MODEL_CONFIG path; then ONNX_MODEL_PATH, META_PATH, etc.
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut cfg: ModelConfig = common::load_yaml(path)?;
        if let Ok(p) = std::env::var("ONNX_MODEL_PATH") {
            cfg.onnx_model_path = p;
        }
        if let Ok(p) = std::env::var("META_PATH") {
            cfg.meta_path = p;
        }
        if let Some(v) = std::env::var("EXPECTED_FEATURE_LEN")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            cfg.expected_feature_len = v;
        }
        if let Some(r) = std::env::var("WARMUP_RUNS")
            .ok()
            .and_then(|v| v.parse().ok())
        {
            cfg.warmup_runs = r;
        }
        if let Ok(k) = std::env::var("OUTPUT_KIND") {
            cfg.output_kind = k;
        }
        Ok(cfg)
    }
}

impl ModelMeta {
    /// Load from JSON path.
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let s = std::fs::read_to_string(path)?;
        let meta: ModelMeta = serde_json::from_str(&s)?;
        Ok(meta)
    }
}
