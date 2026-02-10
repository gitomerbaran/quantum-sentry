//! Offline deterministic replay from ClickHouse or JSONL + dataset export + golden capture.

pub mod engine;
pub mod export;
pub mod golden;
pub mod paper;
pub mod source;
pub mod source_clickhouse;
pub mod source_jsonl;

pub use engine::{run_replay, ReplayResult};
pub use export::{export_run, ExportFormat, GoldenCapture, RunExport};
pub use paper::{run_paper_trading, PaperResult};
pub use golden::{
    compare_decisions, compare_features, float_eq, load_expected_decisions, load_expected_features,
    load_raw_bbo, verify_golden_pack, VerificationResult, ABS_TOL, REL_TOL,
};
pub use source::{ReplayEvent, ReplaySource};
pub use source_clickhouse::ClickHouseReplaySource;
pub use source_jsonl::JsonlReplaySource;
