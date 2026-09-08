//! Wide historical dataset rows (one settled market = one `DsMarket`; its
//! candlestick path = many `DsPrice`). Built by `mbot build-dataset`, consumed
//! in memory by `mbot universe`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DsMarket {
    pub ticker: String,
    pub series: String,
    pub event_ticker: String,
    pub category: String,
    pub frequency: String,
    pub title: String,
    pub open_ts: i64,
    pub close_ts: i64,
    pub result_yes: bool,
    pub volume: f64,
    pub strike_type: String,
    pub floor_strike: Option<f64>,
    pub cap_strike: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DsPrice {
    pub ticker: String,
    /// candle end, unix seconds
    pub ts: i64,
    pub secs_to_close: i64,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub last: Option<f64>,
    pub volume: f64,
    pub open_interest: f64,
}
