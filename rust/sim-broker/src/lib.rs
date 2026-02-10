//! Sim broker: paper trading simulator for OrderCommand execution.

pub mod paper_broker;

pub use paper_broker::{
    AccountSnapshot, Fill, PaperBroker, PaperBrokerConfig, PositionSide, SymbolPortfolio,
};


