//! Quantum Sentinel v2 Hot Path Feature Engine.
//! Scale-invariant features only; RAM-only; no DB. Bounded channels, drop-old keep-latest.

pub mod features;
pub mod ring_buffer;
pub mod runner;
pub mod state;

pub use features::{imbalance, log_return_1, log_return_window, spread_pct};
pub use ring_buffer::RingBuffer;
pub use runner::{channel_pair, run};
pub use state::{FeatureEngine, PerSymbolState};
