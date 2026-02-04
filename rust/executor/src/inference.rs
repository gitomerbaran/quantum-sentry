//! Inference abstraction: trait + Mock (tests). ONNX behind `onnx` feature.

/// Output of inference (p_up, p_down, confidence).
#[derive(Debug, Clone)]
pub struct InferenceOutput {
    pub p_up: f32,
    pub p_down: f32,
    pub confidence: f32,
    /// When true, executor uses reason "inference_error" (e.g. ONNX failure or NaN).
    pub inference_error: bool,
}

impl InferenceOutput {
    pub fn new(p_up: f32, p_down: f32) -> Self {
        let confidence = p_up.max(p_down);
        Self {
            p_up,
            p_down,
            confidence,
            inference_error: false,
        }
    }

    /// For engines that failed (NaN or runtime error); executor will NO_TRADE with inference_error.
    pub fn failed() -> Self {
        Self {
            p_up: 0.0,
            p_down: 0.0,
            confidence: 0.0,
            inference_error: true,
        }
    }
}

/// Inference engine: predict from features. No DB. Send + Sync for multi-thread.
pub trait InferenceEngine: Send + Sync {
    fn predict(&self, features: &[f32]) -> InferenceOutput;

    /// Expected feature length; if Some(len), executor checks snapshot.features.len() == len else NO_TRADE + meta_mismatch.
    fn expected_feature_len(&self) -> Option<usize> {
        None
    }

    /// Model version string for Decision.model_version.
    fn model_version(&self) -> &str {
        "v1-mock"
    }
}

/// Mock engine: deterministic or configurable outputs for tests.
#[derive(Debug, Clone)]
pub struct MockInferenceEngine {
    pub p_up: f32,
    pub p_down: f32,
    /// If Some(len), executor checks snapshot.features.len() == len (for meta_mismatch tests).
    pub expected_feature_len: Option<usize>,
}

impl MockInferenceEngine {
    pub fn new(p_up: f32, p_down: f32) -> Self {
        Self {
            p_up,
            p_down,
            expected_feature_len: None,
        }
    }

    /// Returns LONG bias (p_up > p_down) with high confidence.
    pub fn long_bias() -> Self {
        Self::new(0.7, 0.3)
    }

    /// Returns SHORT bias (p_down > p_up) with high confidence.
    pub fn short_bias() -> Self {
        Self::new(0.3, 0.7)
    }

    /// Returns low confidence (for gate tests).
    pub fn low_confidence() -> Self {
        Self::new(0.5, 0.5)
    }

    /// Deterministic from first feature: if features[0] > 0 then long bias else short bias.
    pub fn from_first_feature() -> Self {
        Self::new(0.0, 0.0) // overridden in predict
    }

    /// Mock that enforces expected feature length (for meta_mismatch test).
    pub fn with_expected_feature_len(self, len: usize) -> Self {
        Self {
            expected_feature_len: Some(len),
            ..self
        }
    }
}

/// Mock that always returns inference_error (for inference_error test).
#[derive(Debug, Clone)]
pub struct MockInferenceEngineFails;

impl MockInferenceEngineFails {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MockInferenceEngineFails {
    fn default() -> Self {
        Self::new()
    }
}

impl InferenceEngine for MockInferenceEngineFails {
    fn predict(&self, _features: &[f32]) -> InferenceOutput {
        InferenceOutput::failed()
    }
}

/// Mock that returns LONG on first call, SHORT on second (for hysteresis flip test).
#[derive(Debug)]
pub struct MockInferenceEngineFlip {
    call_count: std::sync::atomic::AtomicUsize,
}

impl MockInferenceEngineFlip {
    pub fn new() -> Self {
        Self {
            call_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl Default for MockInferenceEngineFlip {
    fn default() -> Self {
        Self::new()
    }
}

impl InferenceEngine for MockInferenceEngineFlip {
    fn predict(&self, _features: &[f32]) -> InferenceOutput {
        let n = self
            .call_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if n == 0 {
            InferenceOutput::new(0.7, 0.3)
        } else {
            InferenceOutput::new(0.3, 0.7)
        }
    }
}

impl InferenceEngine for MockInferenceEngine {
    fn predict(&self, features: &[f32]) -> InferenceOutput {
        let (p_up, p_down) = if self.p_up == 0.0 && self.p_down == 0.0 && !features.is_empty() {
            if features[0] > 0.0 {
                (0.65f32, 0.35f32)
            } else {
                (0.35f32, 0.65f32)
            }
        } else {
            (self.p_up, self.p_down)
        };
        InferenceOutput::new(p_up, p_down)
    }

    fn expected_feature_len(&self) -> Option<usize> {
        self.expected_feature_len
    }
}

/// Type-erased engine for runtime selection (e.g. ONNX vs Mock).
pub struct BoxedInferenceEngine(pub Box<dyn InferenceEngine>);

impl std::fmt::Debug for BoxedInferenceEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BoxedInferenceEngine(..)")
    }
}

impl InferenceEngine for BoxedInferenceEngine {
    fn predict(&self, features: &[f32]) -> InferenceOutput {
        self.0.predict(features)
    }
    fn expected_feature_len(&self) -> Option<usize> {
        self.0.expected_feature_len()
    }
    fn model_version(&self) -> &str {
        self.0.model_version()
    }
}

// OrtInferenceEngine lives in onnx_engine when `onnx` feature is enabled.
