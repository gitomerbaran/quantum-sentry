//! ONNX inference via `ort`. Used when `onnx` feature is enabled.

use std::path::Path;
use std::sync::Mutex;
use std::time::Instant;

use ort::session::Session;
use crate::inference::{InferenceEngine, InferenceOutput};
use crate::model_config::{ModelConfig, ModelMeta};

/// Real ONNX inference engine. Session + meta created once at startup.
#[derive(Debug)]
pub struct OrtInferenceEngine {
    session: Mutex<Session>,
    expected_feature_len: usize,
    model_version: String,
    output_kind: String, // "binary_probs" or "logits"
    /// Preallocated input buffer [1, expected_feature_len] to avoid per-call alloc.
    input_buffer: Mutex<Vec<f32>>,
}

fn softmax_2(logits: &[f32]) -> (f32, f32) {
    if logits.len() < 2 {
        return (0.5, 0.5);
    }
    let a = logits[0];
    let b = logits[1];
    let max = a.max(b);
    let ea = (a - max).exp();
    let eb = (b - max).exp();
    let sum = ea + eb;
    if sum <= 0.0 {
        return (0.5, 0.5);
    }
    (ea / sum, eb / sum)
}

impl OrtInferenceEngine {
    /// Build from model config and meta. Creates ONNX session; returns error if load fails.
    pub fn new(
        config: &ModelConfig,
        meta: &ModelMeta,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let session = Session::builder()?
            .commit_from_file(&config.onnx_model_path)
            .map_err(|e| format!("ONNX session from {}: {}", config.onnx_model_path, e))?;
        let expected_feature_len = meta.expected_feature_len;
        let mut input_buffer = Vec::with_capacity(expected_feature_len);
        input_buffer.resize(expected_feature_len, 0.0_f32);
        Ok(Self {
            session: Mutex::new(session),
            expected_feature_len,
            model_version: meta.model_version.clone(),
            output_kind: config.output_kind.clone(),
            input_buffer: Mutex::new(input_buffer),
        })
    }

    /// Load config and meta from paths, then build engine. Fails if any file or session fails.
    pub fn from_paths(
        model_config_path: &Path,
        _meta_path: &Path,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let config = ModelConfig::load(model_config_path)?;
        let meta = ModelMeta::load(Path::new(&config.meta_path))?;
        Self::new(&config, &meta)
    }

    /// Run warmup_runs inferences with zeros; returns average runtime in ms or error.
    pub fn warmup(&self, runs: u32) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        let features: Vec<f32> = vec![0.0; self.expected_feature_len];
        let start = Instant::now();
        for _ in 0..runs {
            let out = self.predict(&features);
            if out.inference_error {
                return Err("warmup inference failed".into());
            }
        }
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
        Ok(elapsed_ms / runs as f64)
    }
}

impl InferenceEngine for OrtInferenceEngine {
    fn predict(&self, features: &[f32]) -> InferenceOutput {
        let expected = self.expected_feature_len;
        if features.len() != expected {
            return InferenceOutput::failed();
        }

        let mut buf_guard = match self.input_buffer.lock() {
            Ok(b) => b,
            Err(_) => return InferenceOutput::failed(),
        };
        buf_guard[..expected].copy_from_slice(features);
        let shape = [1_usize, expected];
        let data: Box<[f32]> = buf_guard[..expected].to_vec().into_boxed_slice();
        drop(buf_guard);

        let input = match ort::value::Tensor::from_array((shape, data)) {
            Ok(v) => v,
            Err(_) => return InferenceOutput::failed(),
        };

        let mut session_guard: std::sync::MutexGuard<'_, Session> = match self.session.lock() {
            Ok(s) => s,
            Err(_) => return InferenceOutput::failed(),
        };

        let outputs = match session_guard.run(ort::inputs![input]) {
            Ok(o) => o,
            Err(_) => return InferenceOutput::failed(),
        };

        let out_val = &outputs[0];
        let (_, slice) = match out_val.try_extract_tensor::<f32>() {
            Ok(t) => t,
            Err(_) => return InferenceOutput::failed(),
        };

        let (p_down, p_up) = if self.output_kind == "logits" {
            softmax_2(slice)
        } else {
            if slice.len() < 2 {
                return InferenceOutput::failed();
            }
            (slice[0], slice[1])
        };

        if !p_down.is_finite() || !p_up.is_finite() {
            return InferenceOutput::failed();
        }
        InferenceOutput::new(p_up, p_down)
    }

    fn expected_feature_len(&self) -> Option<usize> {
        Some(self.expected_feature_len)
    }

    fn model_version(&self) -> &str {
        &self.model_version
    }
}
