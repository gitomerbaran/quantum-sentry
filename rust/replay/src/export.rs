//! Export replay results to export_dir: features, decisions, summary; optional golden capture.

use common::types::Action;
use common::{BboTick, Decision, FeatureSnapshot};
use serde::Serialize;
use std::io::Write;
use std::path::Path;

use crate::engine::ReplayResult;

/// Export format for features and decisions files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExportFormat {
    #[default]
    Csv,
    Jsonl,
    Parquet,
}

impl std::str::FromStr for ExportFormat {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "csv" => Ok(ExportFormat::Csv),
            "jsonl" => Ok(ExportFormat::Jsonl),
            "parquet" => Ok(ExportFormat::Parquet),
            _ => anyhow::bail!("unknown export format: {}", s),
        }
    }
}

/// Options for a run export.
#[derive(Debug, Clone)]
pub struct RunExport {
    pub export_dir: std::path::PathBuf,
    pub export_format: ExportFormat,
    pub write_decisions: bool,
    /// When true, write raw replayed BBO stream to export_dir/raw_bbo.jsonl.
    pub log_raw_ticks: bool,
}

/// Golden pack capture: write raw BBO, expected features, expected decisions under golden/<name>/.
#[derive(Debug, Clone)]
pub struct GoldenCapture {
    pub name: String,
    pub base_dir: std::path::PathBuf,
}

/// Summary stats written to summary.json.
#[derive(Debug, Serialize)]
pub struct ExportSummary {
    pub num_events: u64,
    pub num_snapshots: u64,
    pub num_decisions: u64,
    pub num_orders: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_latency_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_latency_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_latency_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_spread_pct: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avg_spread_pct: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_spread_pct: Option<f32>,
}

/// Write result to export_dir; optionally write golden pack.
pub fn export_run(
    result: &ReplayResult,
    opts: &RunExport,
    golden: Option<&GoldenCapture>,
) -> anyhow::Result<ExportSummary> {
    std::fs::create_dir_all(&opts.export_dir)?;
    let ext = match opts.export_format {
        ExportFormat::Csv => "csv",
        ExportFormat::Jsonl => "jsonl",
        ExportFormat::Parquet => "parquet",
    };

    write_features(&opts.export_dir, &result.snapshots, opts.export_format, ext)?;
    if opts.write_decisions {
        write_decisions(&opts.export_dir, &result.decisions, opts.export_format, ext)?;
    }
    if opts.log_raw_ticks && !result.bbo_ticks.is_empty() {
        write_golden_raw_bbo(&opts.export_dir, &result.bbo_ticks)?;
    }

    if let Some(g) = golden {
        let golden_dir = g.base_dir.join("golden").join(&g.name);
        std::fs::create_dir_all(&golden_dir)?;
        write_golden_raw_bbo(&golden_dir, &result.bbo_ticks)?;
        write_golden_features(&golden_dir, &result.snapshots)?;
        write_golden_decisions(&golden_dir, &result.decisions)?;
    }

    let summary = build_summary(result);
    let summary_path = opts.export_dir.join("summary.json");
    std::fs::write(summary_path, serde_json::to_string_pretty(&summary)?)?;
    Ok(summary)
}

fn write_features(
    dir: &Path,
    snapshots: &[FeatureSnapshot],
    format: ExportFormat,
    ext: &str,
) -> anyhow::Result<()> {
    if snapshots.is_empty() {
        return Ok(());
    }
    let path = dir.join(format!("features.{}", ext));
    match format {
        ExportFormat::Csv => write_features_csv(&path, snapshots),
        ExportFormat::Jsonl => write_features_jsonl(&path, snapshots),
        ExportFormat::Parquet => write_features_parquet(&path, snapshots),
    }
}

fn write_features_csv(path: &Path, snapshots: &[FeatureSnapshot]) -> anyhow::Result<()> {
    let mut w = csv::Writer::from_path(path)?;
    let n = snapshots.first().map(|s| s.features.len()).unwrap_or(0);
    let mut header: Vec<String> = vec![
        "ts_exchange".to_string(),
        "symbol".to_string(),
        "feature_version".to_string(),
        "spread_pct".to_string(),
        "data_latency_ms".to_string(),
    ];
    for i in 0..n {
        header.push(format!("f{i}"));
    }
    w.write_record(&header)?;
    for s in snapshots {
        let mut row: Vec<String> = vec![
            s.ts_exchange.to_string(),
            s.symbol.clone(),
            s.feature_version.to_string(),
            s.spread_pct.to_string(),
            s.data_latency_ms.to_string(),
        ];
        for &f in &s.features {
            row.push(f.to_string());
        }
        w.write_record(&row)?;
    }
    w.flush()?;
    Ok(())
}

fn write_features_jsonl(path: &Path, snapshots: &[FeatureSnapshot]) -> anyhow::Result<()> {
    let f = std::fs::File::create(path)?;
    let mut w = std::io::BufWriter::new(f);
    for s in snapshots {
        serde_json::to_writer(&mut w, s)?;
        w.write_all(b"\n")?;
    }
    Ok(())
}

fn write_features_parquet(path: &Path, snapshots: &[FeatureSnapshot]) -> anyhow::Result<()> {
    let _ = (path, snapshots);
    anyhow::bail!(
        "parquet export not built in this binary; build with --features parquet or use csv/jsonl"
    )
}

fn write_decisions(
    dir: &Path,
    decisions: &[Decision],
    format: ExportFormat,
    ext: &str,
) -> anyhow::Result<()> {
    if decisions.is_empty() {
        return Ok(());
    }
    let path = dir.join(format!("decisions.{}", ext));
    match format {
        ExportFormat::Csv => write_decisions_csv(&path, decisions),
        ExportFormat::Jsonl => write_decisions_jsonl(&path, decisions),
        ExportFormat::Parquet => write_decisions_parquet(&path, decisions),
    }
}

fn write_decisions_csv(path: &Path, decisions: &[Decision]) -> anyhow::Result<()> {
    let mut w = csv::Writer::from_path(path)?;
    w.write_record([
        "ts_exchange",
        "symbol",
        "action",
        "confidence",
        "model_version",
        "reason_codes",
    ])?;
    for d in decisions {
        let reasons = serde_json::to_string(&d.reason_codes).unwrap_or_else(|_| "[]".to_string());
        let action_str = match d.action {
            Action::NoTrade => "no_trade",
            Action::Long => "long",
            Action::Short => "short",
        };
        w.write_record(&[
            d.ts_exchange.to_string(),
            d.symbol.clone(),
            action_str.to_string(),
            d.confidence.to_string(),
            d.model_version.clone(),
            reasons,
        ])?;
    }
    w.flush()?;
    Ok(())
}

fn write_decisions_jsonl(path: &Path, decisions: &[Decision]) -> anyhow::Result<()> {
    let f = std::fs::File::create(path)?;
    let mut w = std::io::BufWriter::new(f);
    for d in decisions {
        serde_json::to_writer(&mut w, d)?;
        w.write_all(b"\n")?;
    }
    Ok(())
}

fn write_decisions_parquet(path: &Path, decisions: &[Decision]) -> anyhow::Result<()> {
    let _ = (path, decisions);
    anyhow::bail!(
        "parquet export not built in this binary; build with --features parquet or use csv/jsonl"
    )
}

fn write_golden_raw_bbo(dir: &Path, bbos: &[BboTick]) -> anyhow::Result<()> {
    let path = dir.join("raw_bbo.jsonl");
    let f = std::fs::File::create(path)?;
    let mut w = std::io::BufWriter::new(f);
    for b in bbos {
        serde_json::to_writer(&mut w, b)?;
        w.write_all(b"\n")?;
    }
    Ok(())
}

fn write_golden_features(dir: &Path, snapshots: &[FeatureSnapshot]) -> anyhow::Result<()> {
    let path = dir.join("expected_features.jsonl");
    let f = std::fs::File::create(path)?;
    let mut w = std::io::BufWriter::new(f);
    for s in snapshots {
        serde_json::to_writer(&mut w, s)?;
        w.write_all(b"\n")?;
    }
    Ok(())
}

fn write_golden_decisions(dir: &Path, decisions: &[Decision]) -> anyhow::Result<()> {
    let path = dir.join("expected_decisions.jsonl");
    let f = std::fs::File::create(path)?;
    let mut w = std::io::BufWriter::new(f);
    for d in decisions {
        serde_json::to_writer(&mut w, d)?;
        w.write_all(b"\n")?;
    }
    Ok(())
}

fn build_summary(result: &ReplayResult) -> ExportSummary {
    let num_events = result.bbo_ticks.len() as u64;
    let num_snapshots = result.snapshots.len() as u64;
    let num_decisions = result.decisions.len() as u64;
    let num_orders = result.orders.len() as u64;
    let (min_latency_ms, avg_latency_ms, max_latency_ms) = if result.snapshots.is_empty() {
        (None, None, None)
    } else {
        let latencies: Vec<u32> = result.snapshots.iter().map(|s| s.data_latency_ms).collect();
        let min = *latencies.iter().min().unwrap();
        let max = *latencies.iter().max().unwrap();
        let avg = latencies.iter().map(|&x| f64::from(x)).sum::<f64>() / latencies.len() as f64;
        (Some(min), Some(avg), Some(max))
    };
    let (min_spread_pct, avg_spread_pct, max_spread_pct) = if result.snapshots.is_empty() {
        (None, None, None)
    } else {
        let spreads: Vec<f32> = result.snapshots.iter().map(|s| s.spread_pct).collect();
        let min = spreads.iter().cloned().fold(f32::NAN, f32::min);
        let max = spreads.iter().cloned().fold(f32::NAN, f32::max);
        let avg = spreads.iter().map(|&x| f64::from(x)).sum::<f64>() / spreads.len() as f64;
        (Some(min), Some(avg as f32), Some(max))
    };
    ExportSummary {
        num_events,
        num_snapshots,
        num_decisions,
        num_orders,
        min_latency_ms,
        avg_latency_ms,
        max_latency_ms,
        min_spread_pct,
        avg_spread_pct,
        max_spread_pct,
    }
}
