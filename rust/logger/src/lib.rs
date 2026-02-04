//! Quantum Sentinel v2 cold-path async batch logger.
//! MockSink for tests; ClickHouseSink behind `clickhouse` feature.

pub mod config;
pub mod runtime;
pub mod sink;

pub use config::LoggerConfig;
pub use runtime::{run_logger, try_send_log};
pub use sink::{LogSink, LoggerCounters, MockSink};

#[cfg(feature = "clickhouse")]
pub use sink::ClickHouseSink;
