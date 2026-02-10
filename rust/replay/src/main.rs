//! CLI for offline replay: ClickHouse or JSONL -> feature_engine -> executor -> export.

use anyhow::Context;
use clap::Parser;
use replay::engine::run_replay;
use replay::export::{export_run, GoldenCapture, RunExport};
use replay::golden;
use replay::source::ReplaySource;
use replay::{ClickHouseReplaySource, ExportFormat, JsonlReplaySource};
use sim_broker::{AccountSnapshot, Fill};
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
    #[arg(long, required_unless_present_any = ["upgrade_golden", "verify_golden"])]
    pub from: Option<String>,

    /// End time: ISO8601 or epoch milliseconds
    #[arg(long, required_unless_present_any = ["upgrade_golden", "verify_golden"])]
    pub to: Option<String>,

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

    /// Sampling gate: only process events if ts_exchange advanced by >= min_gap_ms
    #[arg(long, default_value = "0")]
    pub min_gap_ms: u64,

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

    /// Override executor latency gate (ms). Useful for offline replay when ts_ingest-ts_exchange is large.
    #[arg(long)]
    pub latency_max_ms: Option<u32>,

    /// Optional executor config file (presets under configs/executor_*.yml).
    /// When set, this replaces risk.yml-derived executor tuning for this replay run.
    #[arg(long)]
    pub executor_config: Option<PathBuf>,

    /// Enable paper trading simulation (default: false)
    #[arg(long, default_value = "false")]
    pub paper: bool,

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

    /// Verify golden pack: replay raw_bbo and compare with expected outputs
    #[arg(long)]
    pub verify_golden: Option<PathBuf>,

    /// Whether to verify decisions (default: true if engine matches meta.json)
    #[arg(long)]
    pub verify_decisions: Option<bool>,

    /// Override inference engine for verify (mock|onnx). Must match meta.json if present.
    #[arg(long)]
    pub engine: Option<String>,

    /// Upgrade legacy golden pack: add seq fields and meta.json
    #[arg(long)]
    pub upgrade_golden: Option<PathBuf>,
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

    // Handle upgrade-golden command
    if let Some(ref golden_dir) = args.upgrade_golden {
        return upgrade_golden_command(golden_dir.clone(), &args).await;
    }

    // Handle verify-golden command
    if let Some(ref golden_dir) = args.verify_golden {
        return verify_golden_command(golden_dir.clone(), &args).await;
    }

    let from_ms = parse_ts(
        args.from
            .as_ref()
            .context("--from is required unless using --upgrade-golden or --verify-golden")?,
    )?;
    let to_ms = parse_ts(
        args.to
            .as_ref()
            .context("--to is required unless using --upgrade-golden or --verify-golden")?,
    )?;

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
            let username = std::env::var("CLICKHOUSE_USER").ok();
            let password = std::env::var("CLICKHOUSE_PASSWORD").ok();
            Box::new(ClickHouseReplaySource {
                url,
                database,
                username,
                password,
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
    let window: usize = std::env::var("FEATURE_WINDOW")
        .ok()
        .and_then(|s| s.parse().ok())
        .or_else(|| {
            features_config
                .features
                .first()
                .and_then(|f| f.window.trim_end_matches('s').parse().ok())
        })
        .unwrap_or(60);
    if window == 0 {
        anyhow::bail!("feature window must be > 0");
    }

    let executor_config = if let Some(ref p) = args.executor_config {
        executor::ExecutorConfig::load_executor_config(p).unwrap_or_else(|e| {
            tracing::warn!(path = %p.display(), error = %e, "executor config load failed; using env/default");
            executor::ExecutorConfig::from_env_or_default()
        })
    } else {
        executor::ExecutorConfig::load(&args.risk_config).unwrap_or_else(|e| {
            tracing::warn!(path = %args.risk_config.display(), error = %e, "risk config load failed; using env/default");
            executor::ExecutorConfig::from_env_or_default()
        })
    };
    let mut executor_config = executor_config;
    if let Some(exec) = args.execution_enabled {
        executor_config.execution_enabled = exec;
    }
    if let Some(lat) = args.latency_max_ms {
        executor_config.latency_max_ms = lat;
    }

    let engine =
        executor::BoxedInferenceEngine(Box::new(executor::MockInferenceEngine::long_bias()));

    // Create meta.json if capturing golden (before engine is moved to run_replay)
    let golden = args.golden_name.as_ref().map(|name| GoldenCapture {
        name: name.clone(),
        base_dir: args.export_dir.clone(),
    });

    let meta = if golden.is_some() {
        let model_version_str = engine.0.model_version();
        let expected_feature_len = engine.0.expected_feature_len().unwrap_or(4);
        let inference_engine = if model_version_str.contains("mock") {
            "mock"
        } else {
            "onnx"
        };
        let feature_order = vec![
            "log_return_1".to_string(),
            "log_return_window".to_string(),
            "spread_pct".to_string(),
            "imbalance".to_string(),
        ];

        // Compute risk config hash
        let risk_config_hash = std::fs::read_to_string(&args.risk_config)
            .ok()
            .map(|content| {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                content.hash(&mut hasher);
                format!("{:x}", hasher.finish())
            });

        Some(golden::GoldenMeta {
            symbol: args.symbol.clone(),
            from_ts_ms: from_ms,
            to_ts_ms: to_ms,
            feature_window: window,
            feature_version: 1,
            feature_order,
            expected_feature_len,
            min_gap_ms: args.min_gap_ms,
            limit: args.limit,
            inference_engine: inference_engine.to_string(),
            risk_config_hash,
            model_version: if inference_engine == "onnx" {
                Some(model_version_str.to_string())
            } else {
                None
            },
        })
    } else {
        None
    };

    info!(symbol = %args.symbol, from = from_ms, to = to_ms, window, "running replay");
    let result = run_replay(
        source.as_ref(),
        window,
        executor_config.clone(),
        engine,
        args.replay_speed,
        executor_config.execution_enabled,
        args.min_gap_ms,
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

    let summary = export_run(&result, &opts, golden.as_ref(), meta.as_ref())?;
    info!(?summary, "export done");

    // Paper trading simulation if enabled
    if args.paper {
        use replay::paper::run_paper_trading;
        use sim_broker::PaperBrokerConfig;

        if result.orders_with_decisions.is_empty() {
            tracing::warn!("paper enabled but no orders were produced (all decisions may be no_trade); sim outputs will be empty");
        }

        let paper_config = PaperBrokerConfig::from_env();
        info!("running paper trading simulation");
        let paper_result = run_paper_trading(
            &result.bbo_ticks,
            &result.orders_with_decisions,
            paper_config,
        );

        // Export paper trading results (always writes sim_summary.json)
        export_paper_results(&opts.export_dir, &paper_result, opts.export_format)?;
        info!(
            fills = paper_result.fills.len(),
            snapshots = paper_result.account_snapshots.len(),
            "paper trading done"
        );
    }

    Ok(())
}

fn export_paper_results(
    export_dir: &std::path::Path,
    paper_result: &replay::PaperResult,
    format: replay::ExportFormat,
) -> anyhow::Result<()> {
    let ext = match format {
        ExportFormat::Csv => "csv",
        ExportFormat::Jsonl => "jsonl",
        ExportFormat::Parquet => "parquet",
    };

    // Export fills
    if !paper_result.fills.is_empty() {
        let path = export_dir.join(format!("sim_fills.{}", ext));
        match format {
            ExportFormat::Csv => write_fills_csv(&path, &paper_result.fills)?,
            ExportFormat::Jsonl => write_fills_jsonl(&path, &paper_result.fills)?,
            ExportFormat::Parquet => {
                anyhow::bail!("parquet export not implemented for paper results")
            }
        }
    }

    // Export account snapshots
    if !paper_result.account_snapshots.is_empty() {
        let path = export_dir.join(format!("sim_account.{}", ext));
        match format {
            ExportFormat::Csv => write_account_csv(&path, &paper_result.account_snapshots)?,
            ExportFormat::Jsonl => write_account_jsonl(&path, &paper_result.account_snapshots)?,
            ExportFormat::Parquet => {
                anyhow::bail!("parquet export not implemented for paper results")
            }
        }
    }

    // Write summary
    let summary = build_paper_summary(paper_result);
    let summary_path = export_dir.join("sim_summary.json");
    std::fs::write(summary_path, serde_json::to_string_pretty(&summary)?)?;

    Ok(())
}

fn write_fills_csv(path: &std::path::Path, fills: &[Fill]) -> anyhow::Result<()> {
    let mut w = csv::Writer::from_path(path)?;
    w.write_record([
        "ts_exchange",
        "symbol",
        "order_id",
        "action",
        "qty",
        "fill_price",
        "notional",
        "fee",
        "slippage_bps",
        "reason_code",
    ])?;
    for f in fills {
        w.write_record(&[
            f.ts_exchange.to_string(),
            f.symbol.clone(),
            f.order_id.clone(),
            f.action.clone(),
            f.qty.to_string(),
            f.fill_price.to_string(),
            f.notional.to_string(),
            f.fee.to_string(),
            f.slippage_bps.to_string(),
            f.reason_code.clone(),
        ])?;
    }
    w.flush()?;
    Ok(())
}

fn write_fills_jsonl(path: &std::path::Path, fills: &[Fill]) -> anyhow::Result<()> {
    use std::io::Write;
    let f = std::fs::File::create(path)?;
    let mut w = std::io::BufWriter::new(f);
    for fill in fills {
        serde_json::to_writer(&mut w, fill)?;
        w.write_all(b"\n")?;
    }
    Ok(())
}

fn write_account_csv(path: &std::path::Path, snapshots: &[AccountSnapshot]) -> anyhow::Result<()> {
    let mut w = csv::Writer::from_path(path)?;
    w.write_record([
        "ts_exchange",
        "symbol",
        "cash_usdt",
        "position_side",
        "position_qty",
        "entry_price",
        "mark_price",
        "unrealized_pnl",
        "realized_pnl",
        "equity_usdt",
        "fees_paid",
        "trades_total",
        "wins_total",
        "losses_total",
    ])?;
    for s in snapshots {
        w.write_record(&[
            s.ts_exchange.to_string(),
            s.symbol.clone(),
            s.cash_usdt.to_string(),
            s.position_side.clone(),
            s.position_qty.to_string(),
            s.entry_price.to_string(),
            s.mark_price.to_string(),
            s.unrealized_pnl.to_string(),
            s.realized_pnl.to_string(),
            s.equity_usdt.to_string(),
            s.fees_paid.to_string(),
            s.trades_total.to_string(),
            s.wins_total.to_string(),
            s.losses_total.to_string(),
        ])?;
    }
    w.flush()?;
    Ok(())
}

fn write_account_jsonl(
    path: &std::path::Path,
    snapshots: &[AccountSnapshot],
) -> anyhow::Result<()> {
    use std::io::Write;
    let f = std::fs::File::create(path)?;
    let mut w = std::io::BufWriter::new(f);
    for snapshot in snapshots {
        serde_json::to_writer(&mut w, snapshot)?;
        w.write_all(b"\n")?;
    }
    Ok(())
}

#[derive(serde::Serialize)]
struct PaperSummary {
    num_fills: usize,
    num_snapshots: usize,
    total_trades: u32,
    total_wins: u32,
    total_losses: u32,
    win_rate: f64,
    initial_cash: f64,
    final_equity: f64,
    total_pnl: f64,
    total_fees: f64,
    max_drawdown_pct: f64,
}

fn build_paper_summary(paper_result: &replay::PaperResult) -> PaperSummary {
    let num_fills = paper_result.fills.len();
    let num_snapshots = paper_result.account_snapshots.len();

    let (total_trades, total_wins, total_losses, total_fees, initial_cash, final_equity) =
        if let Some(last) = paper_result.account_snapshots.last() {
            (
                last.trades_total,
                last.wins_total,
                last.losses_total,
                last.fees_paid,
                paper_result.initial_cash_usdt,
                last.equity_usdt,
            )
        } else {
            (0, 0, 0, 0.0, paper_result.initial_cash_usdt, paper_result.initial_cash_usdt)
        };

    let win_rate = if total_trades > 0 {
        total_wins as f64 / total_trades as f64
    } else {
        0.0
    };

    // Max drawdown over the equity time series (using snapshot order).
    let mut peak = f64::NEG_INFINITY;
    let mut max_dd = 0.0_f64;
    for s in &paper_result.account_snapshots {
        let eq = s.equity_usdt;
        if eq > peak {
            peak = eq;
        }
        if peak.is_finite() && peak > 0.0 {
            let dd = (peak - eq) / peak;
            if dd > max_dd {
                max_dd = dd;
            }
        }
    }

    PaperSummary {
        num_fills,
        num_snapshots,
        total_trades,
        total_wins,
        total_losses,
        win_rate,
        initial_cash,
        final_equity,
        total_pnl: final_equity - initial_cash,
        total_fees,
        max_drawdown_pct: max_dd * 100.0,
    }
}

async fn verify_golden_command(golden_dir: PathBuf, args: &Args) -> anyhow::Result<()> {
    if !golden_dir.exists() {
        anyhow::bail!("Golden directory does not exist: {}", golden_dir.display());
    }

    let features_config = common::load_features(&args.features_config)
        .unwrap_or_else(|e| {
            tracing::warn!(path = %args.features_config.display(), error = %e, "features config load failed; using default window 60");
            common::FeaturesConfig {
                version: 1,
                features: vec![],
            }
        });
    let window: usize = std::env::var("FEATURE_WINDOW")
        .ok()
        .and_then(|s| s.parse().ok())
        .or_else(|| {
            features_config
                .features
                .first()
                .and_then(|f| f.window.trim_end_matches('s').parse().ok())
        })
        .unwrap_or(60);

    let executor_config = executor::ExecutorConfig::load(&args.risk_config).unwrap_or_else(|e| {
        tracing::warn!(path = %args.risk_config.display(), error = %e, "risk config load failed; using env/default");
        executor::ExecutorConfig::from_env_or_default()
    });
    let mut executor_config = executor_config;
    executor_config.execution_enabled = false; // Shadow mode for verification
    if let Some(exec) = args.execution_enabled {
        executor_config.execution_enabled = exec;
    }

    // Determine engine from args or default to mock
    let engine = if let Some(ref engine_str) = args.engine {
        match engine_str.as_str() {
            "mock" => {
                executor::BoxedInferenceEngine(Box::new(executor::MockInferenceEngine::long_bias()))
            }
            "onnx" => {
                // ONNX support would require model config loading
                // For now, fallback to mock with warning
                tracing::warn!(
                    "ONNX engine requested but not fully implemented in verify; using mock"
                );
                executor::BoxedInferenceEngine(Box::new(executor::MockInferenceEngine::long_bias()))
            }
            _ => anyhow::bail!("Unknown engine: {}. Use 'mock' or 'onnx'", engine_str),
        }
    } else {
        executor::BoxedInferenceEngine(Box::new(executor::MockInferenceEngine::long_bias()))
    };

    let verify_decisions = args.verify_decisions.unwrap_or(true);

    info!(path = %golden_dir.display(), window, verify_decisions, "verifying golden pack");
    let result = golden::verify_golden_pack(
        golden_dir,
        window,
        executor_config,
        engine,
        verify_decisions,
    )?;

    if result.is_success() {
        info!(
            features_checked = result.num_features_checked,
            decisions_checked = result.num_decisions_checked,
            "golden pack verification passed"
        );
        Ok(())
    } else {
        result.print_errors();
        anyhow::bail!(
            "Golden pack verification failed: {} feature errors, {} decision errors",
            result.feature_errors.len(),
            result.decision_errors.len()
        );
    }
}

async fn upgrade_golden_command(golden_dir: PathBuf, args: &Args) -> anyhow::Result<()> {
    if !golden_dir.exists() {
        anyhow::bail!("Golden directory does not exist: {}", golden_dir.display());
    }

    let raw_bbo_path = golden_dir.join("raw_bbo.jsonl");
    let expected_features_path = golden_dir.join("expected_features.jsonl");
    let expected_decisions_path = golden_dir.join("expected_decisions.jsonl");
    let meta_path = golden_dir.join("meta.json");

    if meta_path.exists() {
        tracing::info!("meta.json already exists; skipping upgrade");
        return Ok(());
    }

    tracing::info!(path = %golden_dir.display(), "upgrading legacy golden pack");

    // Load existing data
    let raw_bbos = golden::load_raw_bbo(&raw_bbo_path)?;
    let expected_features = golden::load_expected_features(&expected_features_path)?;
    let expected_decisions = golden::load_expected_decisions(&expected_decisions_path).ok();

    // Determine window: try to extract from golden pack name (e.g. test_1hour_w60 -> 60)
    // or from features config or env, default to 60
    let window: usize = golden_dir
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|s| {
            // Look for pattern like "w60" or "_w60" in the name
            s.rfind('w').and_then(|pos| {
                let rest = &s[pos + 1..];
                let num_str: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
                num_str.parse().ok()
            })
        })
        .or_else(|| {
            std::env::var("FEATURE_WINDOW")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .or_else(|| {
            common::load_features(&args.features_config)
                .ok()
                .and_then(|cfg| {
                    cfg.features
                        .first()
                        .and_then(|f| f.window.trim_end_matches('s').parse().ok())
                })
        })
        .unwrap_or(60);

    // Create meta.json with best-effort values
    // For legacy packs, we don't have symbol/from/to/limit info, so use placeholders
    let meta = golden::GoldenMeta {
        symbol: "UNKNOWN".to_string(), // Legacy: unknown
        from_ts_ms: 0,                 // Legacy: unknown
        to_ts_ms: 0,                   // Legacy: unknown
        feature_window: window,
        feature_version: 1,
        feature_order: vec![
            "log_return_1".to_string(),
            "log_return_window".to_string(),
            "spread_pct".to_string(),
            "imbalance".to_string(),
        ],
        expected_feature_len: expected_features
            .first()
            .map(|(_, snap)| snap.features.len())
            .unwrap_or(4),
        min_gap_ms: 0,                        // Unknown, use default
        limit: None,                          // Unknown, assume no limit
        inference_engine: "mock".to_string(), // Assume mock for legacy
        risk_config_hash: None,
        model_version: None,
    };

    golden::write_meta(&meta_path, &meta)?;
    tracing::info!("Created meta.json");

    // Rewrite files with seq fields if they don't have them
    // Check if first line has seq field
    let needs_upgrade = {
        let content = std::fs::read_to_string(&raw_bbo_path).ok();
        content
            .as_ref()
            .and_then(|c| c.lines().next())
            .and_then(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .map(|v| v.get("seq").is_none())
            .unwrap_or(true)
    };

    if needs_upgrade {
        // Rewrite with seq fields
        use common::{BboTick, Decision, FeatureSnapshot};
        use replay::export;

        let bbos: Vec<BboTick> = raw_bbos.iter().map(|(_, tick)| tick.clone()).collect();
        export::write_golden_raw_bbo_with_seq(&golden_dir, &bbos)?;

        let snaps: Vec<FeatureSnapshot> = expected_features
            .iter()
            .map(|(_, snap)| snap.clone())
            .collect();
        export::write_golden_features_with_seq(&golden_dir, &snaps)?;

        if let Some(ref decs) = expected_decisions {
            let decs: Vec<Decision> = decs.iter().map(|(_, dec)| dec.clone()).collect();
            export::write_golden_decisions_with_seq(&golden_dir, &decs)?;
        }

        tracing::info!("Rewrote golden files with seq fields");
    }

    Ok(())
}
