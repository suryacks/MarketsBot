//! Polymarket read-side client: Gamma (market metadata), CLOB (books, prices,
//! history) and the public market WebSocket. Order signing (EIP-712) is not
//! implemented yet — Polymarket is a data/arb-signal source for now.

pub mod clob;
pub mod gamma;
pub mod ws;

pub use clob::ClobClient;
pub use gamma::{GammaClient, GammaEvent, GammaMarket};
pub use ws::PolymarketWs;

pub const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";
pub const CLOB_BASE: &str = "https://clob.polymarket.com";
pub const WS_MARKET: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
