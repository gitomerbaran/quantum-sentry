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
    let r1 = run_replay(&source, window, config.clone(), engine1, 0.0, false, 0)
        .await
        .unwrap();
    let engine2 = BoxedInferenceEngine(Box::new(MockInferenceEngine::long_bias()));
    let r2 = run_replay(&source, window, config, engine2, 0.0, false, 0)
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
    let result = run_replay(&source, window, config, engine, 0.0, false, 0)
        .await
        .unwrap();

    let export_dir = dir.path().join("out");
    let opts = RunExport {
        export_dir: export_dir.clone(),
        export_format: ExportFormat::Csv,
        write_decisions: true,
        log_raw_ticks: false,
    };
    let summary = export_run(&result, &opts, None, None).unwrap();

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

    // Decision reason histogram
    let reasons_path = export_dir.join("decision_reasons.json");
    let reasons_json = std::fs::read_to_string(&reasons_path).unwrap();
    let _: serde_json::Value = serde_json::from_str(&reasons_json).unwrap();
}

#[tokio::test]
async fn aggressive_tuning_produces_some_trades_in_replay() {
    // This is a smoke test: with relaxed latency/spread and low cooldown, we should not be 100% NO_TRADE.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.jsonl");
    // Use high latency (1000ms) to ensure default config would NO_TRADE, but aggressive tuning passes.
    let ticks = [
        bbo(1_700_000_000_000, 1_700_000_001_000, 1, 50000.0, 50001.0),
        bbo(1_700_000_000_100, 1_700_000_001_100, 2, 50000.0, 50001.0),
        bbo(1_700_000_000_200, 1_700_000_001_200, 3, 50000.5, 50001.5),
    ];
    let jsonl = ticks
        .iter()
        .map(|t| serde_json::to_string(&replay::ReplayEvent::Bbo(t.clone())).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&path, jsonl).unwrap();

    let source = JsonlReplaySource::new(&path);
    let window = 2;
    let mut config = ExecutorConfig::from_env_or_default();
    config.latency_max_ms = 5_000;
    config.spread_max_pct = 1.0;
    config.confidence_min = 0.40;
    config.cooldown_ms = 0;
    config.hysteresis_enter = 0.50;
    config.hysteresis_exit = 0.45;
    config.allow_short = true;

    let engine = BoxedInferenceEngine(Box::new(MockInferenceEngine::long_bias()));
    let result = run_replay(&source, window, config, engine, 0.0, false, 0)
        .await
        .unwrap();

    assert!(
        result.decisions.iter().any(|d| d.action != common::Action::NoTrade),
        "expected at least one non-no_trade decision under aggressive tuning"
    );
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
    let result = run_replay(&source, window, config, engine, 0.0, false, 0)
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
    export_run(&result, &opts, Some(&golden), None).unwrap();

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

#[tokio::test]
async fn min_gap_ms_filters_events() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.jsonl");
    // Create BBOs with 1ms, 2ms, 3ms gaps
    let ticks = [
        bbo(1000, 1005, 1, 50000.0, 50001.0),
        bbo(1001, 1006, 2, 50000.0, 50001.0), // 1ms gap
        bbo(1003, 1008, 3, 50000.5, 50001.5), // 2ms gap from previous
        bbo(1006, 1011, 4, 50001.0, 50002.0), // 3ms gap from previous
    ];
    let jsonl = ticks
        .iter()
        .map(|t| serde_json::to_string(&replay::ReplayEvent::Bbo(t.clone())).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&path, jsonl).unwrap();
    let source = JsonlReplaySource::new(&path);
    let window = 2;
    let config = ExecutorConfig::from_env_or_default();
    let engine = BoxedInferenceEngine(Box::new(MockInferenceEngine::long_bias()));

    // With min_gap_ms=2, should skip the 1001 tick (1ms gap) but keep 1003 (2ms gap) and 1006 (3ms gap)
    let result = run_replay(&source, window, config, engine, 0.0, false, 2)
        .await
        .unwrap();

    // Should have processed 3 ticks: 1000 (first), 1003 (2ms gap), 1006 (3ms gap)
    assert!(result.bbo_ticks.len() >= 3);
    assert_eq!(result.bbo_ticks[0].ts_exchange, 1000);
    // The filtered result should skip 1001
    let processed_ts: Vec<i64> = result.bbo_ticks.iter().map(|t| t.ts_exchange).collect();
    assert!(
        !processed_ts.contains(&1001),
        "min_gap_ms=2 should skip 1001 (1ms gap)"
    );
}

#[tokio::test]
async fn golden_capture_with_min_gap_is_consistent() {
    // Test that golden capture with min_gap_ms writes only filtered ticks to raw_bbo.jsonl
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.jsonl");
    
    // Create BBOs with varying gaps
    let ticks = [
        bbo(1000, 1005, 1, 50000.0, 50001.0),
        bbo(1001, 1006, 2, 50000.0, 50001.0), // 1ms gap - should be filtered
        bbo(1003, 1008, 3, 50000.5, 50001.5), // 2ms gap - should pass
        bbo(1005, 1010, 4, 50001.0, 50002.0), // 2ms gap - should pass
        bbo(1008, 1013, 5, 50001.5, 50002.5), // 3ms gap - should pass
    ];
    let jsonl = ticks
        .iter()
        .map(|t| serde_json::to_string(&replay::ReplayEvent::Bbo(t.clone())).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&path, jsonl).unwrap();
    
    let source = JsonlReplaySource::new(&path);
    let window = 2;
    let config = ExecutorConfig::from_env_or_default();
    let engine = BoxedInferenceEngine(Box::new(MockInferenceEngine::long_bias()));
    
    // Run with min_gap_ms=2
    let result = run_replay(&source, window, config.clone(), engine, 0.0, false, 2)
        .await
        .unwrap();
    
    // Capture golden pack
    let export_dir = dir.path().join("out");
    let opts = RunExport {
        export_dir: export_dir.clone(),
        export_format: ExportFormat::Jsonl,
        write_decisions: true,
        log_raw_ticks: false,
    };
    
    // Create meta with min_gap_ms=2
    use replay::golden::GoldenMeta;
    let meta = GoldenMeta {
        symbol: "BTCUSDT".to_string(),
        from_ts_ms: 1000,
        to_ts_ms: 1008,
        feature_window: window,
        feature_version: 1,
        feature_order: vec![
            "log_return_1".to_string(),
            "log_return_window".to_string(),
            "spread_pct".to_string(),
            "imbalance".to_string(),
        ],
        expected_feature_len: 4,
        min_gap_ms: 2,
        limit: None,
        inference_engine: "mock".to_string(),
        risk_config_hash: None,
        model_version: None,
    };
    
    let golden = GoldenCapture {
        name: "test_min_gap".to_string(),
        base_dir: export_dir.clone(),
    };
    export_run(&result, &opts, Some(&golden), Some(&meta)).unwrap();
    
    // Verify: raw_bbo.jsonl should contain only filtered ticks (1000, 1003, 1005, 1008)
    let golden_dir = export_dir.join("golden").join("test_min_gap");
    let raw_bbo_content = std::fs::read_to_string(golden_dir.join("raw_bbo.jsonl")).unwrap();
    let raw_bbos: Vec<_> = raw_bbo_content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|line| {
            let golden: replay::golden::GoldenBbo = serde_json::from_str(line).unwrap();
            golden.tick.ts_exchange
        })
        .collect();
    
    // Should have 4 ticks: 1000 (first), 1003, 1005, 1008 (all with >=2ms gaps)
    assert_eq!(raw_bbos.len(), 4);
    assert_eq!(raw_bbos[0], 1000);
    assert_eq!(raw_bbos[1], 1003);
    assert_eq!(raw_bbos[2], 1005);
    assert_eq!(raw_bbos[3], 1008);
    assert!(!raw_bbos.contains(&1001), "1001 should be filtered out (1ms gap)");
    
    // Verify that verify_golden_pack works with this golden pack
    let verify_engine = BoxedInferenceEngine(Box::new(MockInferenceEngine::long_bias()));
    let verify_result = replay::golden::verify_golden_pack(
        golden_dir,
        window,
        config,
        verify_engine,
        true,
    )
    .unwrap();
    
    assert!(verify_result.is_success(), "Golden verification should pass");
    assert_eq!(verify_result.num_raw_events, 4);
    assert_eq!(verify_result.num_snapshots_expected, verify_result.num_snapshots_produced);
}
