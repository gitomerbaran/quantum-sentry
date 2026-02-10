//! Golden pack verification: compare replay outputs with expected golden outputs.

use common::{BboTick, Decision, FeatureSnapshot};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Float tolerance constants for comparison.
pub const ABS_TOL: f32 = 1e-6;
pub const REL_TOL: f32 = 1e-6;

/// Golden pack metadata (required for new packs, optional for legacy).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoldenMeta {
    #[serde(default = "default_symbol")]
    pub symbol: String,
    #[serde(default)]
    pub from_ts_ms: i64,
    #[serde(default)]
    pub to_ts_ms: i64,
    pub feature_window: usize,
    pub feature_version: u32,
    pub feature_order: Vec<String>,
    pub expected_feature_len: usize,
    #[serde(default)]
    pub min_gap_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub limit: Option<u64>,
    pub inference_engine: String, // "mock" or "onnx"
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub risk_config_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub model_version: Option<String>,
}

fn default_symbol() -> String {
    "UNKNOWN".to_string()
}

/// Golden BBO with sequence number.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoldenBbo {
    pub seq: u64,
    #[serde(flatten)]
    pub tick: BboTick,
}

/// Golden feature snapshot with sequence number.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoldenFeature {
    pub seq: u64,
    #[serde(flatten)]
    pub snapshot: FeatureSnapshot,
}

/// Golden decision with sequence number.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoldenDecision {
    pub seq: u64,
    #[serde(flatten)]
    pub decision: Decision,
}

/// Load raw BBO ticks from golden pack (with seq, fallback to line index for legacy).
pub fn load_raw_bbo(path: impl AsRef<Path>) -> anyhow::Result<Vec<(u64, BboTick)>> {
    let content = std::fs::read_to_string(path)?;
    let mut ticks = Vec::new();
    for (line_idx, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // Try to parse as GoldenBbo (with seq)
        if let Ok(golden) = serde_json::from_str::<GoldenBbo>(line) {
            ticks.push((golden.seq, golden.tick));
        } else {
            // Legacy: no seq, use line index
            let tick: BboTick = serde_json::from_str(line)?;
            ticks.push((line_idx as u64, tick));
        }
    }
    Ok(ticks)
}

/// Load expected features from golden pack (with seq, fallback to line index for legacy).
pub fn load_expected_features(
    path: impl AsRef<Path>,
) -> anyhow::Result<Vec<(u64, FeatureSnapshot)>> {
    let content = std::fs::read_to_string(path)?;
    let mut snapshots = Vec::new();
    for (line_idx, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // Try to parse as GoldenFeature (with seq)
        if let Ok(golden) = serde_json::from_str::<GoldenFeature>(line) {
            snapshots.push((golden.seq, golden.snapshot));
        } else {
            // Legacy: no seq, use line index
            let snap: FeatureSnapshot = serde_json::from_str(line)?;
            snapshots.push((line_idx as u64, snap));
        }
    }
    Ok(snapshots)
}

/// Load expected decisions from golden pack (with seq, fallback to line index for legacy).
pub fn load_expected_decisions(path: impl AsRef<Path>) -> anyhow::Result<Vec<(u64, Decision)>> {
    let content = std::fs::read_to_string(path)?;
    let mut decisions = Vec::new();
    for (line_idx, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // Try to parse as GoldenDecision (with seq)
        if let Ok(golden) = serde_json::from_str::<GoldenDecision>(line) {
            decisions.push((golden.seq, golden.decision));
        } else {
            // Legacy: no seq, use line index
            let dec: Decision = serde_json::from_str(line)?;
            decisions.push((line_idx as u64, dec));
        }
    }
    Ok(decisions)
}

/// Load golden metadata (required for new packs).
pub fn load_meta(path: impl AsRef<Path>) -> anyhow::Result<Option<GoldenMeta>> {
    if !path.as_ref().exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(path)?;
    let meta: GoldenMeta = serde_json::from_str(&content)?;
    Ok(Some(meta))
}

/// Write golden metadata.
pub fn write_meta(path: impl AsRef<Path>, meta: &GoldenMeta) -> anyhow::Result<()> {
    let content = serde_json::to_string_pretty(meta)?;
    std::fs::write(path, content)?;
    Ok(())
}

/// Check if two floats are within tolerance.
pub fn float_eq(a: f32, b: f32) -> bool {
    let diff = (a - b).abs();
    if diff <= ABS_TOL {
        return true;
    }
    let max_abs = a.abs().max(b.abs());
    if max_abs > 0.0 {
        diff / max_abs <= REL_TOL
    } else {
        diff <= ABS_TOL
    }
}

/// Compare two feature snapshots with tolerances.
pub fn compare_features(
    expected: &FeatureSnapshot,
    actual: &FeatureSnapshot,
    seq: u64,
) -> Result<(), String> {
    if expected.ts_exchange != actual.ts_exchange {
        return Err(format!(
            "seq {}: ts_exchange mismatch: expected {}, actual {}",
            seq, expected.ts_exchange, actual.ts_exchange
        ));
    }
    if expected.symbol != actual.symbol {
        return Err(format!(
            "seq {}: symbol mismatch: expected {}, actual {}",
            seq, expected.symbol, actual.symbol
        ));
    }
    if expected.ready != actual.ready {
        return Err(format!(
            "seq {}: ready mismatch: expected {}, actual {}",
            seq, expected.ready, actual.ready
        ));
    }
    if expected.data_latency_ms != actual.data_latency_ms {
        return Err(format!(
            "seq {}: data_latency_ms mismatch: expected {}, actual {}",
            seq, expected.data_latency_ms, actual.data_latency_ms
        ));
    }
    if !float_eq(expected.spread_pct, actual.spread_pct) {
        return Err(format!(
            "seq {}: spread_pct mismatch: expected {}, actual {} (diff: {})",
            seq,
            expected.spread_pct,
            actual.spread_pct,
            (expected.spread_pct - actual.spread_pct).abs()
        ));
    }
    if expected.feature_version != actual.feature_version {
        return Err(format!(
            "seq {}: feature_version mismatch: expected {}, actual {}",
            seq, expected.feature_version, actual.feature_version
        ));
    }
    if expected.features.len() != actual.features.len() {
        return Err(format!(
            "seq {}: features length mismatch: expected {}, actual {}",
            seq,
            expected.features.len(),
            actual.features.len()
        ));
    }
    for (i, (exp_f, act_f)) in expected
        .features
        .iter()
        .zip(actual.features.iter())
        .enumerate()
    {
        if !float_eq(*exp_f, *act_f) {
            return Err(format!(
                "seq {}: feature[{}] mismatch: expected {}, actual {} (diff: {})",
                seq,
                i,
                exp_f,
                act_f,
                (exp_f - act_f).abs()
            ));
        }
    }
    Ok(())
}

/// Compare two decisions with tolerances.
pub fn compare_decisions(expected: &Decision, actual: &Decision, seq: u64) -> Result<(), String> {
    if expected.ts_exchange != actual.ts_exchange {
        return Err(format!(
            "seq {}: ts_exchange mismatch: expected {}, actual {}",
            seq, expected.ts_exchange, actual.ts_exchange
        ));
    }
    if expected.symbol != actual.symbol {
        return Err(format!(
            "seq {}: symbol mismatch: expected {}, actual {}",
            seq, expected.symbol, actual.symbol
        ));
    }
    if expected.action != actual.action {
        return Err(format!(
            "seq {}: action mismatch: expected {:?}, actual {:?}",
            seq, expected.action, actual.action
        ));
    }
    if !float_eq(expected.confidence, actual.confidence) {
        return Err(format!(
            "seq {}: confidence mismatch: expected {}, actual {} (diff: {})",
            seq,
            expected.confidence,
            actual.confidence,
            (expected.confidence - actual.confidence).abs()
        ));
    }
    if expected.model_version != actual.model_version {
        return Err(format!(
            "seq {}: model_version mismatch: expected {}, actual {}",
            seq, expected.model_version, actual.model_version
        ));
    }
    // Reason codes: exact match
    if expected.reason_codes != actual.reason_codes {
        return Err(format!(
            "seq {}: reason_codes mismatch: expected {:?}, actual {:?}",
            seq, expected.reason_codes, actual.reason_codes
        ));
    }
    Ok(())
}

/// Result type for synchronous verification (snapshots and decisions with seq).
type VerifyResult = (Vec<(u64, FeatureSnapshot)>, Vec<(u64, Decision)>);

/// Synchronous verify: process BBOs directly without channels (no drops).
/// Creates Executor with a new engine matching the provided one's type.
/// For golden tests, the engine type should match meta.json inference_engine.
/// This ensures deterministic verification without any channel drops or backpressure.
#[allow(clippy::type_complexity)]
fn verify_synchronous(
    raw_bbos: &[(u64, BboTick)],
    window: usize,
    executor_config: executor::ExecutorConfig,
    engine: &executor::BoxedInferenceEngine,
    min_gap_ms: u64,
) -> anyhow::Result<VerifyResult> {
    use executor::executor::Executor;
    use feature_engine::state::FeatureEngine;

    let mut fe = FeatureEngine::new(window);

    // Create a new engine matching the provided one's type
    // Since BoxedInferenceEngine doesn't implement Clone, we create a new one
    // based on the model_version string to determine the engine type
    let model_version = engine.0.model_version();
    let expected_len = engine.0.expected_feature_len();

    // Determine engine type from model_version
    // For mock engines, model_version typically contains "mock"
    // For ONNX engines, we'd need model config, but for golden tests we use mock
    let engine_for_exec = if model_version.contains("mock") {
        let mut mock_engine = executor::MockInferenceEngine::long_bias();
        if let Some(len) = expected_len {
            mock_engine = mock_engine.with_expected_feature_len(len);
        }
        mock_engine
    } else {
        // For ONNX or other engines, we'd need to load from config
        // For now, fallback to mock with warning (golden tests typically use mock)
        tracing::warn!(
            model_version = %model_version,
            "Non-mock engine detected in verify_synchronous; using mock fallback. \
             For ONNX verification, ensure engine type matches meta.json"
        );
        let mut mock_engine = executor::MockInferenceEngine::long_bias();
        if let Some(len) = expected_len {
            mock_engine = mock_engine.with_expected_feature_len(len);
        }
        mock_engine
    };

    let mut exec = Executor::new(executor_config.clone(), engine_for_exec);

    let mut snapshots = Vec::new();
    let mut decisions = Vec::new();
    let mut snapshot_seq = 0u64;
    let mut last_processed_ts: Option<i64> = None;

    for (_, tick) in raw_bbos {
        // Apply min_gap_ms gate (same as run_replay)
        // In run_replay, this gate is applied BEFORE sending to channel
        if min_gap_ms > 0 {
            if let Some(last) = last_processed_ts {
                let delta_ms = (tick.ts_exchange - last).max(0) as u64;
                if delta_ms < min_gap_ms {
                    continue; // Skip this BBO (same as run_replay)
                }
            }
        }

        // Process BBO (same as run_replay: send to feature engine)
        // In run_replay, last_processed_ts is updated AFTER successful send
        // Here we update it after processing (which is equivalent)
        if let Some(snap) = fe.on_bbo(tick) {
            // Use Executor's on_snapshot method directly (applies all gates and stabilization)
            let (dec, _order_opt) = exec.on_snapshot(&snap);

            snapshots.push((snapshot_seq, snap));
            decisions.push((snapshot_seq, dec));
            snapshot_seq += 1;
        }
        // Update last_processed_ts after processing (same as run_replay)
        // This ensures min_gap_ms gate works identically
        last_processed_ts = Some(tick.ts_exchange);
    }

    Ok((snapshots, decisions))
}

/// Verify golden pack: synchronous replay and compare with expected outputs.
pub fn verify_golden_pack(
    golden_dir: impl AsRef<Path>,
    window: usize,
    executor_config: executor::ExecutorConfig,
    engine: executor::BoxedInferenceEngine,
    verify_decisions: bool,
) -> anyhow::Result<VerificationResult> {
    let golden_dir = golden_dir.as_ref();
    let raw_bbo_path = golden_dir.join("raw_bbo.jsonl");
    let expected_features_path = golden_dir.join("expected_features.jsonl");
    let expected_decisions_path = golden_dir.join("expected_decisions.jsonl");
    let meta_path = golden_dir.join("meta.json");

    // Load meta.json and enforce config parity
    let meta = load_meta(&meta_path)?;
    let is_legacy = meta.is_none();
    if is_legacy {
        tracing::warn!(
            "Legacy golden pack detected (no meta.json). Using line index as seq fallback."
        );
    }

    if let Some(ref m) = meta {
        // Enforce feature_window match
        if m.feature_window != window {
            anyhow::bail!(
                "Config mismatch: meta.json feature_window={}, current window={}",
                m.feature_window,
                window
            );
        }

        // Enforce expected_feature_len match
        if let Some(expected_len) = engine.0.expected_feature_len() {
            if m.expected_feature_len != expected_len {
                anyhow::bail!(
                    "Config mismatch: meta.json expected_feature_len={}, engine expects {}",
                    m.expected_feature_len,
                    expected_len
                );
            }
        } else if m.expected_feature_len != 4 {
            // Default feature length is 4 (log_return_1, log_return_window, spread_pct, imbalance)
            anyhow::bail!(
                "Config mismatch: meta.json expected_feature_len={}, but engine has no explicit expectation (defaults to 4)",
                m.expected_feature_len
            );
        }

        // Check inference engine match for decision verification
        let current_engine = if engine.0.model_version().contains("mock") {
            "mock"
        } else {
            "onnx"
        };

        // Decision verification: if engine doesn't match, skip with warning (per prompt)
        // Prompt says: "either skip decision checks with a warning OR provide env var to force ONNX verify"
        if verify_decisions && m.inference_engine != current_engine {
            tracing::warn!(
                "Inference engine mismatch: meta.json says '{}', current is '{}'. Skipping decision verification. Use matching engine or --verify-decisions false to suppress this warning.",
                m.inference_engine,
                current_engine
            );
            // Will skip decision verification below
        }

        // Enforce feature_order match (per prompt: "feature_order matches")
        let expected_order = vec![
            "log_return_1".to_string(),
            "log_return_window".to_string(),
            "spread_pct".to_string(),
            "imbalance".to_string(),
        ];
        if m.feature_order != expected_order {
            anyhow::bail!(
                "Config mismatch: meta.json feature_order={:?}, expected {:?}",
                m.feature_order,
                expected_order
            );
        }
    }

    let expected_features = load_expected_features(&expected_features_path)?;

    // Determine if we should actually verify decisions (engine must match if meta present)
    let should_verify_decisions = if let Some(ref m) = meta {
        let current_engine = if engine.0.model_version().contains("mock") {
            "mock"
        } else {
            "onnx"
        };
        verify_decisions && m.inference_engine == current_engine
    } else {
        verify_decisions // Legacy: verify if flag is set
    };

    let expected_decisions = if should_verify_decisions {
        load_expected_decisions(&expected_decisions_path)?
    } else {
        Vec::new()
    };

    // Load raw BBOs
    let raw_bbos = load_raw_bbo(&raw_bbo_path)?;

    // Sanity check: assert ts_exchange monotonicity (per prompt)
    for i in 1..raw_bbos.len() {
        if raw_bbos[i].1.ts_exchange < raw_bbos[i - 1].1.ts_exchange {
            anyhow::bail!(
                "ts_exchange not monotonic: seq {} has ts_exchange {} < seq {} ts_exchange {}",
                raw_bbos[i].0,
                raw_bbos[i].1.ts_exchange,
                raw_bbos[i - 1].0,
                raw_bbos[i - 1].1.ts_exchange
            );
        }
    }

    // Synchronous replay (no channels, no drops)
    // Use min_gap_ms from meta.json (enforced above) or 0 for legacy packs
    let min_gap_ms = meta.as_ref().map(|m| m.min_gap_ms).unwrap_or(0);
    let (actual_snapshots, actual_decisions) = verify_synchronous(
        &raw_bbos,
        window,
        executor_config.clone(),
        &engine,
        min_gap_ms,
    )?;

    // Early count assertions (fail fast before detailed comparison)
    if expected_features.len() != actual_snapshots.len() {
        let first_5_expected: Vec<_> = expected_features.iter().take(5).collect();
        let first_5_actual: Vec<_> = actual_snapshots.iter().take(5).collect();
        anyhow::bail!(
            "Count mismatch: expected {} snapshots, got {}. \
             This indicates filter/config mismatch. \
             Expected first 5 seqs: {:?}, Actual first 5 seqs: {:?}. \
             Check meta.json min_gap_ms={}, limit={:?} matches capture settings.",
            expected_features.len(),
            actual_snapshots.len(),
            first_5_expected
                .iter()
                .map(|(seq, _)| seq)
                .collect::<Vec<_>>(),
            first_5_actual
                .iter()
                .map(|(seq, _)| seq)
                .collect::<Vec<_>>(),
            min_gap_ms,
            meta.as_ref().and_then(|m| m.limit)
        );
    }

    if should_verify_decisions && expected_decisions.len() != actual_decisions.len() {
        let first_5_expected: Vec<_> = expected_decisions.iter().take(5).collect();
        let first_5_actual: Vec<_> = actual_decisions.iter().take(5).collect();
        anyhow::bail!(
            "Decision count mismatch: expected {} decisions, got {}. \
             This indicates filter/config mismatch. \
             Expected first 5 seqs: {:?}, Actual first 5 seqs: {:?}. \
             Check meta.json min_gap_ms={}, limit={:?} matches capture settings.",
            expected_decisions.len(),
            actual_decisions.len(),
            first_5_expected
                .iter()
                .map(|(seq, _)| seq)
                .collect::<Vec<_>>(),
            first_5_actual
                .iter()
                .map(|(seq, _)| seq)
                .collect::<Vec<_>>(),
            min_gap_ms,
            meta.as_ref().and_then(|m| m.limit)
        );
    }

    // Compare by seq
    let mut feature_errors = Vec::new();
    let mut decision_errors = Vec::new();

    // Features comparison - match by seq
    let expected_by_seq: HashMap<u64, &FeatureSnapshot> = expected_features
        .iter()
        .map(|(seq, snap)| (*seq, snap))
        .collect();
    let actual_by_seq: HashMap<u64, &FeatureSnapshot> = actual_snapshots
        .iter()
        .map(|(seq, snap)| (*seq, snap))
        .collect();

    // Check all expected features
    for (seq, exp_snap) in &expected_features {
        match actual_by_seq.get(seq) {
            Some(act_snap) => {
                if let Err(e) = compare_features(exp_snap, act_snap, *seq) {
                    feature_errors.push(e);
                }
            }
            None => {
                feature_errors.push(format!(
                    "seq {}: not found in actual snapshots (ts_exchange={})",
                    seq, exp_snap.ts_exchange
                ));
            }
        }
    }

    // Check for extra snapshots
    for (seq, act_snap) in &actual_snapshots {
        if !expected_by_seq.contains_key(seq) {
            feature_errors.push(format!(
                "seq {}: unexpected snapshot in actual (ts_exchange={})",
                seq, act_snap.ts_exchange
            ));
        }
    }

    // Decisions comparison - only if should_verify_decisions is true (already computed above)
    if should_verify_decisions {
        let expected_dec_by_seq: HashMap<u64, &Decision> = expected_decisions
            .iter()
            .map(|(seq, dec)| (*seq, dec))
            .collect();
        let actual_dec_by_seq: HashMap<u64, &Decision> = actual_decisions
            .iter()
            .map(|(seq, dec)| (*seq, dec))
            .collect();

        if expected_decisions.len() != actual_decisions.len() {
            decision_errors.push(format!(
                "Count mismatch: expected {} decisions, got {}",
                expected_decisions.len(),
                actual_decisions.len()
            ));
        }

        for (seq, exp_dec) in &expected_decisions {
            match actual_dec_by_seq.get(seq) {
                Some(act_dec) => {
                    if let Err(e) = compare_decisions(exp_dec, act_dec, *seq) {
                        decision_errors.push(e);
                    }
                }
                None => {
                    decision_errors.push(format!(
                        "seq {}: not found in actual decisions (ts_exchange={})",
                        seq, exp_dec.ts_exchange
                    ));
                }
            }
        }

        for (seq, act_dec) in &actual_decisions {
            if !expected_dec_by_seq.contains_key(seq) {
                decision_errors.push(format!(
                    "seq {}: unexpected decision in actual (ts_exchange={})",
                    seq, act_dec.ts_exchange
                ));
            }
        }
    }

    Ok(VerificationResult {
        feature_errors,
        decision_errors,
        num_features_checked: expected_features.len().min(actual_snapshots.len()),
        num_decisions_checked: if should_verify_decisions {
            expected_decisions.len().min(actual_decisions.len())
        } else {
            0
        },
        num_raw_events: raw_bbos.len(),
        num_snapshots_expected: expected_features.len(),
        num_snapshots_produced: actual_snapshots.len(),
        num_decisions_expected: if should_verify_decisions {
            expected_decisions.len()
        } else {
            0
        },
        num_decisions_produced: if should_verify_decisions {
            actual_decisions.len()
        } else {
            0
        },
        is_legacy,
        meta: meta.clone(),
    })
}

/// Result of golden pack verification.
#[derive(Debug)]
pub struct VerificationResult {
    pub feature_errors: Vec<String>,
    pub decision_errors: Vec<String>,
    pub num_features_checked: usize,
    pub num_decisions_checked: usize,
    pub num_raw_events: usize,
    pub num_snapshots_expected: usize,
    pub num_snapshots_produced: usize,
    pub num_decisions_expected: usize,
    pub num_decisions_produced: usize,
    pub is_legacy: bool,
    pub meta: Option<GoldenMeta>,
}

impl VerificationResult {
    pub fn is_success(&self) -> bool {
        self.feature_errors.is_empty() && self.decision_errors.is_empty()
    }

    pub fn print_errors(&self) {
        eprintln!("╔═══════════════════════════════════════════════════════════════╗");
        eprintln!("║           Golden Pack Verification Summary                   ║");
        eprintln!("╚═══════════════════════════════════════════════════════════════╝");
        eprintln!();
        if let Some(ref meta) = self.meta {
            eprintln!("Meta (from golden pack):");
            eprintln!("  Symbol:                {}", meta.symbol);
            eprintln!(
                "  Time range:            {} to {}",
                meta.from_ts_ms, meta.to_ts_ms
            );
            eprintln!("  Feature window:        {}", meta.feature_window);
            eprintln!("  Min gap (ms):          {}", meta.min_gap_ms);
            if let Some(limit) = meta.limit {
                eprintln!("  Limit:                 {}", limit);
            }
            eprintln!("  Inference engine:      {}", meta.inference_engine);
            eprintln!();
        }
        eprintln!("Counts:");
        eprintln!("  Raw BBO events:        {}", self.num_raw_events);
        eprintln!(
            "  Feature snapshots:     expected {}, produced {}",
            self.num_snapshots_expected, self.num_snapshots_produced
        );
        if self.num_decisions_expected > 0 || self.num_decisions_produced > 0 {
            eprintln!(
                "  Decisions:             expected {}, produced {}",
                self.num_decisions_expected, self.num_decisions_produced
            );
        }
        eprintln!("  Features checked:      {}", self.num_features_checked);
        if self.num_decisions_checked > 0 {
            eprintln!("  Decisions checked:      {}", self.num_decisions_checked);
        }
        eprintln!();

        if self.is_legacy {
            eprintln!("⚠️  WARNING: Legacy golden pack (no meta.json, using line index as seq)");
            eprintln!("   Consider running: replay --upgrade-golden <path>");
            eprintln!();
        }

        if !self.feature_errors.is_empty() {
            eprintln!("❌ Feature Mismatches ({}):", self.feature_errors.len());
            eprintln!("─────────────────────────────────────────────────────────────");
            // Show first 20 errors with more detail
            for (idx, err) in self.feature_errors.iter().take(20).enumerate() {
                eprintln!("  [{:3}] {}", idx + 1, err);
            }
            if self.feature_errors.len() > 20 {
                eprintln!(
                    "  ... and {} more feature errors",
                    self.feature_errors.len() - 20
                );
            }
            eprintln!();
        }

        if !self.decision_errors.is_empty() {
            eprintln!("❌ Decision Mismatches ({}):", self.decision_errors.len());
            eprintln!("─────────────────────────────────────────────────────────────");
            // Show first 20 errors with more detail
            for (idx, err) in self.decision_errors.iter().take(20).enumerate() {
                eprintln!("  [{:3}] {}", idx + 1, err);
            }
            if self.decision_errors.len() > 20 {
                eprintln!(
                    "  ... and {} more decision errors",
                    self.decision_errors.len() - 20
                );
            }
            eprintln!();
        }

        if self.feature_errors.is_empty() && self.decision_errors.is_empty() {
            eprintln!("✅ All checks passed!");
        }
    }
}
