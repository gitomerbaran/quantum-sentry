//! CLI for offline replay: ClickHouse or JSONL -> feature_engine -> executor -> export.

use anyhow::Context;
use clap::Parser;
use replay::engine::run_replay;
use replay::export::{export_run, GoldenCapture, RunExport};
use replay::source::ReplaySource;
use replay::{ClickHouseReplaySource, ExportFormat, JsonlReplaySource};
use std::path::PathBuf;
use std::str::FromStr;
use tracing::info;

#[derive(Parser, Debug)]
#[command(
    name = "replay",
    about = "Offline deterministic replay + dataset export"
)]
pub struct Args {
    #[arg(long, default_value = "BTCUSDT")]
    pub symbol: String,

    /// Start time: ISO8601 (e.g. 2026-01-01T00:00:00Z) or epoch milliseconds
    #[arg(long)]
    pub from: String,

    /// End time: ISO8601 or epoch milliseconds
    #[arg(long)]
    pub to: String,

    #[arg(long, value_parser = ["clickhouse", "jsonl"], default_value = "clickhouse")]
    pub source: String,

    /// Path to JSONL file when source=jsonl
    #[arg(long)]
    pub jsonl_path: Option<PathBuf>,

    #[arg(long, default_value = "exports/run_001")]
    pub export_dir: PathBuf,

    #[arg(long, value_parser = ["parquet", "csv", "jsonl"], default_value = "csv")]
    pub export_format: String,

    /// 0 = as-fast-as-possible; >0 = real-time multiplier
    #[arg(long, default_value = "0")]
    pub replay_speed: f64,

    #[arg(long)]
    pub limit: Option<u64>,

    #[arg(long, default_value = "false")]
    pub log_raw_ticks: bool,

    #[arg(long, default_value = "true")]
    pub write_decisions: bool,

    /// Golden pack name; when set, write golden/<name>/raw_bbo.jsonl, expected_*.jsonl
    #[arg(long)]
    pub golden_name: Option<String>,

    /// Override execution_enabled (risk config)
    #[arg(long)]
    pub execution_enabled: Option<bool>,

    #[arg(long, default_value = "configs/features.yml")]
    pub features_config: PathBuf,

    #[arg(long, default_value = "configs/risk.yml")]
    pub risk_config: PathBuf,

    /// Override model config path (for future ONNX replay)
    #[arg(long)]
    pub model_config: Option<PathBuf>,

    /// Override model meta path (for future ONNX replay)
    #[arg(long)]
    pub model_meta: Option<PathBuf>,
}

fn parse_ts(s: &str) -> anyhow::Result<i64> {
    if let Ok(ms) = s.parse::<i64>() {
        return Ok(ms);
    }
    let dt = chrono::DateTime::parse_from_rfc3339(s)
        .context("parse --from/--to as epoch ms or ISO8601 (e.g. 2026-01-01T00:00:00Z)")?;
    Ok(dt.timestamp_millis())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .compact()
        .init();

    let args = Args::parse();

    let from_ms = parse_ts(&args.from)?;
    let to_ms = parse_ts(&args.to)?;

    let source: Box<dyn ReplaySource + Send + Sync> = match args.source.as_str() {
        "clickhouse" => {
            let url = std::env::var("CLICKHOUSE_URL")
                .unwrap_or_else(|_| "http://localhost:8123".to_string());
            let database = std::env::var("CLICKHOUSE_DB")
                .or_else(|_| {
                    std::env::var("CLICKHOUSE_DATABASE").inspect(|_| {
                        tracing::warn!("CLICKHOUSE_DATABASE is deprecated, use CLICKHOUSE_DB");
                    })
                })
                .unwrap_or_else(|_| "quantum".to_string());
            Box::new(ClickHouseReplaySource {
                url,
                database,
                symbol: args.symbol.clone(),
                from_ts_ms: from_ms,
                to_ts_ms: to_ms,
                limit: args.limit,
                include_trades: false,
            })
        }
        "jsonl" => {
            let path = args
                .jsonl_path
                .context("--source jsonl requires --jsonl_path")?;
            Box::new(JsonlReplaySource::new(path))
        }
        _ => anyhow::bail!("unsupported source: {}", args.source),
    };

    let features_config = common::load_features(&args.features_config)
        .unwrap_or_else(|e| {
            tracing::warn!(path = %args.features_config.display(), error = %e, "features config load failed; using default window 60");
            common::FeaturesConfig {
                version: 1,
                features: vec![],
            }
        });
    let window: usize = features_config
        .features
        .first()
        .and_then(|f| f.window.trim_end_matches('s').parse().ok())
        .unwrap_or(60);
    if window == 0 {
        anyhow::bail!("feature window must be > 0");
    }

    let executor_config = executor::ExecutorConfig::load(&args.risk_config).unwrap_or_else(|e| {
        tracing::warn!(path = %args.risk_config.display(), error = %e, "risk config load failed; using env/default");
        executor::ExecutorConfig::from_env_or_default()
    });
    let mut executor_config = executor_config;
    if let Some(exec) = args.execution_enabled {
        executor_config.execution_enabled = exec;
    }

    let engine =
        executor::BoxedInferenceEngine(Box::new(executor::MockInferenceEngine::long_bias()));

    info!(symbol = %args.symbol, from = from_ms, to = to_ms, window, "running replay");
    let result = run_replay(
        source.as_ref(),
        window,
        executor_config.clone(),
        engine,
        args.replay_speed,
        executor_config.execution_enabled,
    )
    .await?;

    info!(
        bbos = result.bbo_ticks.len(),
        snapshots = result.snapshots.len(),
        decisions = result.decisions.len(),
        "replay done"
    );

    let format = ExportFormat::from_str(&args.export_format).unwrap_or(ExportFormat::Csv);
    let opts = RunExport {
        export_dir: args.export_dir.clone(),
        export_format: format,
        write_decisions: args.write_decisions,
        log_raw_ticks: args.log_raw_ticks,
    };
    let golden = args.golden_name.as_ref().map(|name| GoldenCapture {
        name: name.clone(),
        base_dir: args.export_dir.clone(),
    });
    let summary = export_run(&result, &opts, golden.as_ref())?;
    info!(?summary, "export done");

    Ok(())
}
