//! Integration tests: no ClickHouse. Use JsonlReplaySource.

use common::BboTick;
use executor::{BoxedInferenceEngine, ExecutorConfig, MockInferenceEngine};
use replay::engine::run_replay;
use replay::export::{export_run, ExportFormat, GoldenCapture, RunExport};
use replay::JsonlReplaySource;

fn bbo(ts_ex: i64, ts_ing: i64, update_id: u64, bid: f64, ask: f64) -> BboTick {
    BboTick {
        ts_exchange: ts_ex,
        ts_ingest: ts_ing,
        update_id,
        symbol: "BTCUSDT".to_string(),
        bid_price: bid,
        bid_qty: 1.0,
        ask_price: ask,
        ask_qty: 1.0,
    }
}

/// Build a JSONL with enough BBOs to get at least one snapshot (window=2).
fn make_jsonl_bbos() -> String {
    let ticks = [
        bbo(1000, 1005, 1, 50000.0, 50001.0),
        bbo(1001, 1006, 2, 50000.0, 50001.0),
        bbo(1002, 1007, 3, 50000.5, 50001.5),
    ];
    ticks
        .iter()
        .map(|t| serde_json::to_string(&replay::ReplayEvent::Bbo(t.clone())).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn deterministic_same_input_same_decisions() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.jsonl");
    std::fs::write(&path, make_jsonl_bbos()).unwrap();
    let source = JsonlReplaySource::new(&path);
    let window = 2;
    let config = ExecutorConfig::from_env_or_default();
    let engine1 = BoxedInferenceEngine(Box::new(MockInferenceEngine::long_bias()));
    let r1 = run_replay(&source, window, config.clone(), engine1, 0.0, false)
        .await
        .unwrap();
    let engine2 = BoxedInferenceEngine(Box::new(MockInferenceEngine::long_bias()));
    let r2 = run_replay(&source, window, config, engine2, 0.0, false)
        .await
        .unwrap();

    assert_eq!(
        r1.decisions.len(),
        r2.decisions.len(),
        "same number of decisions"
    );
    for (a, b) in r1.decisions.iter().zip(r2.decisions.iter()) {
        assert_eq!(a.ts_exchange, b.ts_exchange);
        assert_eq!(a.action, b.action);
        assert_eq!(a.confidence, b.confidence);
        assert_eq!(a.reason_codes, b.reason_codes);
    }
}

#[tokio::test]
async fn exporter_writes_expected_files_and_columns() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.jsonl");
    std::fs::write(&path, make_jsonl_bbos()).unwrap();
    let source = JsonlReplaySource::new(&path);
    let window = 2;
    let config = ExecutorConfig::from_env_or_default();
    let engine = BoxedInferenceEngine(Box::new(MockInferenceEngine::long_bias()));
    let result = run_replay(&source, window, config, engine, 0.0, false)
        .await
        .unwrap();

    let export_dir = dir.path().join("out");
    let opts = RunExport {
        export_dir: export_dir.clone(),
        export_format: ExportFormat::Csv,
        write_decisions: true,
        log_raw_ticks: false,
    };
    let summary = export_run(&result, &opts, None).unwrap();

    assert!(summary.num_snapshots > 0);
    assert!(summary.num_decisions > 0);

    let features_path = export_dir.join("features.csv");
    let features_csv = std::fs::read_to_string(&features_path).unwrap();
    let mut lines = features_csv.lines();
    let header = lines.next().unwrap();
    assert!(header.contains("ts_exchange"));
    assert!(header.contains("symbol"));
    assert!(header.contains("spread_pct"));
    assert!(header.contains("data_latency_ms"));
    assert!(header.contains("f0"));

    let decisions_path = export_dir.join("decisions.csv");
    let decisions_csv = std::fs::read_to_string(&decisions_path).unwrap();
    let mut dlines = decisions_csv.lines();
    let dheader = dlines.next().unwrap();
    assert!(dheader.contains("ts_exchange"));
    assert!(dheader.contains("action"));
    assert!(dheader.contains("confidence"));
    assert!(dheader.contains("reason_codes"));

    let summary_path = export_dir.join("summary.json");
    let summary_json = std::fs::read_to_string(&summary_path).unwrap();
    let _: serde_json::Value = serde_json::from_str(&summary_json).unwrap();
}

#[tokio::test]
async fn golden_capture_writes_expected_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.jsonl");
    std::fs::write(&path, make_jsonl_bbos()).unwrap();
    let source = JsonlReplaySource::new(&path);
    let window = 2;
    let config = ExecutorConfig::from_env_or_default();
    let engine = BoxedInferenceEngine(Box::new(MockInferenceEngine::long_bias()));
    let result = run_replay(&source, window, config, engine, 0.0, false)
        .await
        .unwrap();

    let export_dir = dir.path().join("out");
    let opts = RunExport {
        export_dir: export_dir.clone(),
        export_format: ExportFormat::Jsonl,
        write_decisions: true,
        log_raw_ticks: false,
    };
    let golden = GoldenCapture {
        name: "test_golden".to_string(),
        base_dir: export_dir.clone(),
    };
    export_run(&result, &opts, Some(&golden)).unwrap();

    let golden_dir = export_dir.join("golden").join("test_golden");
    assert!(golden_dir.join("raw_bbo.jsonl").exists());
    assert!(golden_dir.join("expected_features.jsonl").exists());
    assert!(golden_dir.join("expected_decisions.jsonl").exists());

    let raw_count = std::fs::read_to_string(golden_dir.join("raw_bbo.jsonl"))
        .unwrap()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .count();
    assert_eq!(raw_count, result.bbo_ticks.len());
}
