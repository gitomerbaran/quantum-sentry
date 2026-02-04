//! Quantum Sentinel v2 Executor: gates, stabilization, shadow mode.
//! Consumes FeatureSnapshot, produces Decision (+ optional OrderCommand when execution_enabled).

pub mod config;
pub mod executor;
pub mod inference;
pub mod model_config;
#[cfg(feature = "onnx")]
pub mod onnx_engine;
pub mod runner;

pub use config::ExecutorConfig;
pub use executor::Executor;
pub use inference::{
    BoxedInferenceEngine, InferenceEngine, InferenceOutput, MockInferenceEngine,
    MockInferenceEngineFails, MockInferenceEngineFlip,
};
pub use model_config::{ModelConfig, ModelMeta};
#[cfg(feature = "onnx")]
pub use onnx_engine::OrtInferenceEngine;
pub use runner::{channel_pair, run};
