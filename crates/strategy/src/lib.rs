//! Trading strategies. Everything here is venue-agnostic and runs identically
//! in the backtester, the paper trader and live.

pub mod arb;
pub mod basis;
pub mod btc15m;
pub mod config;
pub mod fair_value;
pub mod flow;
pub mod rules;

pub use flow::{FlowConfig, FlowStrategy};
pub mod spread_maker;
pub mod vol;
pub mod weather_lock;

pub use weather_lock::{WeatherLock, WeatherLockConfig};

pub use rules::{RuleTrader, RuleTraderConfig};

pub use btc15m::Btc15mStrategy;
pub use config::Btc15mConfig;
pub use spread_maker::{SpreadMaker, SpreadMakerConfig};
