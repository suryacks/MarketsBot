//! Kalshi Trade API v2 client: REST (public + authenticated), RSA-PSS request
//! signing, and the WebSocket market-data feed normalized into `MarketEvent`s.

pub mod auth;
pub mod rest;
pub mod types;
pub mod ws;

pub use auth::KalshiAuth;
pub use rest::{KalshiClient, MarketsQuery};
pub use ws::{Channel, KalshiWs};

pub const PROD_REST: &str = "https://external-api.kalshi.com/trade-api/v2";
pub const DEMO_REST: &str = "https://external-api.demo.kalshi.co/trade-api/v2";
pub const PROD_WS: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
pub const DEMO_WS: &str = "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2";

/// Resolve (rest, ws) base URLs from `KALSHI_ENV` (`prod` | `demo`, default demo).
pub fn env_urls() -> (&'static str, &'static str) {
    match std::env::var("KALSHI_ENV").as_deref() {
        Ok("prod") | Ok("production") => (PROD_REST, PROD_WS),
        _ => (DEMO_REST, DEMO_WS),
    }
}
