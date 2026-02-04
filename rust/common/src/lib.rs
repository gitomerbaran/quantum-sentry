//! Quantum Sentinel v2 shared contract crate.
//! Single source of truth for event types and enums used across Gateway, Feature Engine, Executor.

pub mod config;
pub mod events;
pub mod types;

pub use config::{
    load_features, load_risk, load_symbols, load_yaml, FeatureEntry, FeaturesConfig, RiskConfig,
    SymbolsConfig,
};
pub use events::{BboTick, Decision, FeatureSnapshot, LogEvent, OrderCommand, TradeTick};
pub use types::{Action, OrderSide, OrderType};
