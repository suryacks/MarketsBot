//! Serde models for the Kalshi REST API (only the fields we use; everything
//! else is ignored so schema additions don't break us).

use chrono::{DateTime, Utc};
use mb_core::{Fp, MarketInfo, Outcome, Trade, Venue};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct Market {
    pub ticker: String,
    #[serde(default)]
    pub event_ticker: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub market_type: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub result: String,
    pub open_time: Option<DateTime<Utc>>,
    pub close_time: Option<DateTime<Utc>>,
    pub expiration_time: Option<DateTime<Utc>>,
    pub expected_expiration_time: Option<DateTime<Utc>>,
    pub strike_type: Option<String>,
    pub floor_strike: Option<f64>,
    pub cap_strike: Option<f64>,
    pub yes_bid_dollars: Option<Fp>,
    pub yes_ask_dollars: Option<Fp>,
    pub no_bid_dollars: Option<Fp>,
    pub no_ask_dollars: Option<Fp>,
    pub last_price_dollars: Option<Fp>,
    pub yes_bid_size_fp: Option<Fp>,
    pub yes_ask_size_fp: Option<Fp>,
    pub volume_fp: Option<Fp>,
    pub volume_24h_fp: Option<Fp>,
    pub open_interest_fp: Option<Fp>,
    pub settlement_value_dollars: Option<Fp>,
    #[serde(default)]
    pub expiration_value: String,
    pub settlement_ts: Option<DateTime<Utc>>,
}

impl Market {
    /// Series ticker is the prefix of the event ticker (`KXBTC15M-26SEP071445` → `KXBTC15M`).
    pub fn series_ticker(&self) -> String {
        self.event_ticker
            .split('-')
            .next()
            .unwrap_or(&self.event_ticker)
            .to_string()
    }

    pub fn to_info(&self) -> MarketInfo {
        let ts = |t: &Option<DateTime<Utc>>| t.map(|d| d.timestamp_millis()).unwrap_or(0);
        MarketInfo {
            venue: Venue::Kalshi.as_str().to_string(),
            ticker: self.ticker.clone(),
            event_ticker: self.event_ticker.clone(),
            series: self.series_ticker(),
            title: self.title.clone(),
            strike_type: self.strike_type.clone().unwrap_or_default(),
            floor_strike: self.floor_strike,
            cap_strike: self.cap_strike,
            open_ts_ms: ts(&self.open_time),
            close_ts_ms: ts(&self.close_time),
            expiration_ts_ms: ts(&self.expected_expiration_time).max(ts(&self.expiration_time)),
            status: self.status.clone(),
            result: self.result.clone(),
            settlement_value: self.expiration_value.parse::<f64>().ok(),
            yes_bid: self.yes_bid_dollars,
            yes_ask: self.yes_ask_dollars,
            volume: self.volume_fp.unwrap_or(Fp::ZERO),
            category: String::new(),
            open_px: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketsResponse {
    #[serde(default)]
    pub markets: Vec<Market>,
    #[serde(default)]
    pub cursor: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketResponse {
    pub market: Market,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Series {
    pub ticker: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub frequency: String,
    pub fee_type: Option<String>,
    pub fee_multiplier: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SeriesResponse {
    pub series: Series,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SeriesListResponse {
    #[serde(default)]
    pub series: Vec<Series>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct OrderbookFp {
    #[serde(default)]
    pub yes_dollars: Vec<(Fp, Fp)>,
    #[serde(default)]
    pub no_dollars: Vec<(Fp, Fp)>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderbookResponse {
    #[serde(default)]
    pub orderbook_fp: OrderbookFp,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradeRec {
    pub trade_id: String,
    pub ticker: String,
    pub count_fp: Fp,
    pub yes_price_dollars: Fp,
    #[serde(default)]
    pub no_price_dollars: Option<Fp>,
    #[serde(default)]
    pub taker_outcome_side: Option<String>,
    #[serde(default)]
    pub taker_side: Option<String>,
    pub created_time: DateTime<Utc>,
    #[serde(default)]
    pub is_block_trade: bool,
}

impl TradeRec {
    pub fn to_trade(&self) -> Trade {
        let side = self
            .taker_outcome_side
            .as_deref()
            .or(self.taker_side.as_deref())
            .unwrap_or("yes");
        Trade {
            venue: Venue::Kalshi,
            ticker: self.ticker.clone(),
            ts_ms: self.created_time.timestamp_millis(),
            yes_px: self.yes_price_dollars,
            qty: self.count_fp,
            taker: if side == "no" { Outcome::No } else { Outcome::Yes },
            trade_id: self.trade_id.clone(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradesResponse {
    #[serde(default)]
    pub trades: Vec<TradeRec>,
    #[serde(default)]
    pub cursor: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct Ohlc {
    pub open_dollars: Option<Fp>,
    pub high_dollars: Option<Fp>,
    pub low_dollars: Option<Fp>,
    pub close_dollars: Option<Fp>,
    pub mean_dollars: Option<Fp>,
    pub previous_dollars: Option<Fp>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Candle {
    pub end_period_ts: i64,
    #[serde(default)]
    pub price: Ohlc,
    #[serde(default)]
    pub yes_bid: Ohlc,
    #[serde(default)]
    pub yes_ask: Ohlc,
    pub volume_fp: Option<Fp>,
    pub open_interest_fp: Option<Fp>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CandlesResponse {
    #[serde(default)]
    pub ticker: String,
    #[serde(default)]
    pub candlesticks: Vec<Candle>,
}

/// POST /portfolio/events/orders (V2). `side` is `bid` (buy YES) or `ask` (sell YES);
/// `price` is the YES price in dollars.
#[derive(Debug, Clone, Serialize)]
pub struct CreateOrderRequest {
    pub ticker: String,
    pub side: String,
    pub count: String,
    pub price: String,
    pub time_in_force: String,
    pub self_trade_prevention_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub post_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration_time: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateOrderResponse {
    pub order_id: String,
    pub fill_count: Option<Fp>,
    pub remaining_count: Option<Fp>,
    pub average_fill_price: Option<Fp>,
    pub average_fee_paid: Option<Fp>,
    pub ts_ms: Option<i64>,
    pub client_order_id: Option<String>,
}
