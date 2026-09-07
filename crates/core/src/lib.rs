//! Core domain types shared by every crate: fixed-point numbers, orderbooks,
//! market events, fee models and the `Strategy` / `Context` traits that let the
//! same strategy code run in the backtester, the paper trader and (later) live.

pub mod book;
pub mod fees;
pub mod fp;
pub mod strategy;
pub mod types;

pub use book::{Orderbook, Sweep};
pub use fees::FeeModel;
pub use fp::Fp;
pub use strategy::{Action, Context, Fill, OrderId, OrderRequest, Position, Strategy, Tif};
pub use types::{BookSide, MarketEvent, MarketInfo, Outcome, RefPrice, Trade, Venue};
