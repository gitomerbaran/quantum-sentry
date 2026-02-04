//! Minimal wiring: snapshot source -> Executor -> Decision channel.
//! No DB. No exchange. Shadow mode by default (no OrderCommand).
//! Engine: ONNX when `onnx` feature + model exists; else Mock.

use common::FeatureSnapshot;
use executor::{run, BoxedInferenceEngine, ExecutorConfig, MockInferenceEngine};
use logger::{run_logger, LoggerConfig, LoggerCounters, MockSink};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};

#[cfg(feature = "onnx")]
use executor::{ModelConfig, ModelMeta, OrtInferenceEngine};
#[cfg(feature = "onnx")]
use std::path::Path;
#[cfg(feature = "onnx")]
use tracing::warn;

/// Choose inference engine: ONNX when feature + model exists and warmup ok; else Mock.
fn choose_engine() -> (BoxedInferenceEngine, &'static str) {
    #[cfg(feature = "onnx")]
    {
        let model_config_path = std::env::var("MODEL_CONFIG")
            .ok()
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("configs/model.yml"));
        let Ok(model_config) = ModelConfig::load(&model_config_path) else {
            warn!(path = %model_config_path.display(), "model config load failed; using mock");
            return (
                BoxedInferenceEngine(Box::new(MockInferenceEngine::long_bias())),
                "mock",
            );
        };
        if !Path::new(&model_config.onnx_model_path).exists() {
            warn!(
                path = %model_config.onnx_model_path,
                "ONNX model file missing; using mock"
            );
            return (
                BoxedInferenceEngine(Box::new(MockInferenceEngine::long_bias())),
                "mock",
            );
        }
        let Ok(meta) = ModelMeta::load(Path::new(&model_config.meta_path)) else {
            warn!(path = %model_config.meta_path, "model meta load failed; using mock");
            return (
                BoxedInferenceEngine(Box::new(MockInferenceEngine::long_bias())),
                "mock",
            );
        };
        let Ok(ort_engine) = OrtInferenceEngine::new(&model_config, &meta) else {
            warn!("OrtInferenceEngine build failed; using mock");
            return (
                BoxedInferenceEngine(Box::new(MockInferenceEngine::long_bias())),
                "mock",
            );
        };
        match ort_engine.warmup(model_config.warmup_runs) {
            Err(e) => {
                warn!(error = %e, "ONNX warmup failed; using mock");
                return (
                    BoxedInferenceEngine(Box::new(MockInferenceEngine::long_bias())),
                    "mock",
                );
            }
            Ok(avg_ms) => {
                info!(
                    warmup_runs = model_config.warmup_runs,
                    avg_ms = %avg_ms,
                    "warmup ok"
                );
            }
        }
        (BoxedInferenceEngine(Box::new(ort_engine)), "onnx")
    }
    #[cfg(not(feature = "onnx"))]
    {
        (
            BoxedInferenceEngine(Box::new(MockInferenceEngine::long_bias())),
            "mock",
        )
    }
}

fn mock_snapshots() -> Vec<FeatureSnapshot> {
    vec![
        FeatureSnapshot {
            ts_exchange: 1000,
            symbol: "BTCUSDT".to_string(),
            features: vec![0.01, 0.0, 0.02, 0.5],
            ready: true,
            data_latency_ms: 50,
            spread_pct: 0.01,
            feature_version: 1,
        },
        FeatureSnapshot {
            ts_exchange: 1001,
            symbol: "BTCUSDT".to_string(),
            features: vec![0.02, 0.0, 0.02, 0.5],
            ready: true,
            data_latency_ms: 50,
            spread_pct: 0.01,
            feature_version: 1,
        },
    ]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).compact().init();

    let config = std::env::var("EXECUTOR_CONFIG")
        .ok()
        .and_then(|path| ExecutorConfig::load(std::path::Path::new(&path)).ok())
        .unwrap_or_else(ExecutorConfig::from_env_or_default);

    let engine = choose_engine();
    info!(engine = engine.1, "inference engine active");

    let (tx_snap, rx_snap) = mpsc::channel(64);
    let (tx_dec, mut rx_dec) = mpsc::channel(1024);
    let (tx_ord, _rx_ord) = mpsc::channel(512);

    let logger_config = LoggerConfig::from_env_or_default();
    let (tx_log, rx_log) = mpsc::channel(32_000);
    let logger_counters = Arc::new(LoggerCounters::new());
    let sink = Arc::new(MockSink::new());
    tokio::spawn(run_logger(
        rx_log,
        logger_config.clone(),
        sink,
        logger_counters.clone(),
    ));

    let engine = engine.0;
    let tx_order_opt = if config.execution_enabled {
        Some(tx_ord)
    } else {
        None
    };
    let exec_handle = tokio::spawn(async move {
        run(
            rx_snap,
            tx_dec,
            tx_order_opt,
            config,
            engine,
            Some(tx_log),
            logger_config.drop_on_full,
            Some(logger_counters),
        )
        .await;
    });

    let tx_snap_for_producer = tx_snap.clone();
    let producer_handle = tokio::spawn(async move {
        for snap in mock_snapshots() {
            if tx_snap_for_producer.send(snap).await.is_err() {
                break;
            }
        }
    });

    let consumer_handle = tokio::spawn(async move {
        while let Some(dec) = rx_dec.recv().await {
            info!(
                symbol = %dec.symbol,
                action = ?dec.action,
                confidence = dec.confidence,
                reasons = ?dec.reason_codes,
                "decision"
            );
        }
    });

    producer_handle.await?;
    drop(tx_snap);
    exec_handle.await?;
    // tx_dec was moved into run(); it is dropped when the executor task completes
    consumer_handle.await?;

    Ok(())
}
