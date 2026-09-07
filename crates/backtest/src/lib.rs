//! Event-driven backtester. The same `SimExchange` is also used by the paper
//! trader against live data, so fill logic is shared.

pub mod engine;
pub mod history;
pub mod report;
pub mod sim;

pub use engine::Backtester;
pub use report::{MarketPnl, Report};
pub use sim::{FillMode, SimConfig, SimExchange};
