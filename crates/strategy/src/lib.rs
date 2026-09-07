//! Trading strategies. Everything here is venue-agnostic and runs identically
//! in the backtester, the paper trader and live.

pub mod arb;
pub mod btc15m;
pub mod config;
pub mod fair_value;
pub mod vol;

pub use btc15m::Btc15mStrategy;
pub use config::Btc15mConfig;
