use crate::fp::Fp;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Venue {
    Kalshi,
    Polymarket,
}

impl Venue {
    pub fn as_str(self) -> &'static str {
        match self {
            Venue::Kalshi => "kalshi",
            Venue::Polymarket => "polymarket",
        }
    }
    pub fn parse(s: &str) -> Option<Venue> {
        match s {
            "kalshi" => Some(Venue::Kalshi),
            "polymarket" => Some(Venue::Polymarket),
            _ => None,
        }
    }
}

/// Which side of a binary contract.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Yes,
    No,
}

impl Outcome {
    pub fn flip(self) -> Outcome {
        match self {
            Outcome::Yes => Outcome::No,
            Outcome::No => Outcome::Yes,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Yes => "yes",
            Outcome::No => "no",
        }
    }
}

/// Side of the *YES* orderbook. A NO bid at p is a YES ask at 1-p, and every
/// book in the system is normalized to YES terms.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Trade {
    pub venue: Venue,
    pub ticker: String,
    pub ts_ms: i64,
    /// Price paid per YES contract (NO trades are expressed as 1 - no_price).
    pub yes_px: Fp,
    pub qty: Fp,
    /// The aggressor bought this outcome.
    pub taker: Outcome,
    pub trade_id: String,
}

/// Static + slow-moving description of a market. Emitted when a market is
/// discovered and whenever its metadata changes.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MarketInfo {
    pub venue: String,
    pub ticker: String,
    pub event_ticker: String,
    pub series: String,
    pub title: String,
    pub strike_type: String,
    pub floor_strike: Option<f64>,
    pub cap_strike: Option<f64>,
    pub open_ts_ms: i64,
    pub close_ts_ms: i64,
    pub expiration_ts_ms: i64,
    pub status: String,
    /// "yes" | "no" | "" while unsettled
    pub result: String,
    pub settlement_value: Option<f64>,
    pub yes_bid: Option<Fp>,
    pub yes_ask: Option<Fp>,
    pub volume: Fp,
    /// Kalshi category of the series (e.g. "Economics"); empty if unknown.
    #[serde(default)]
    pub category: String,
    /// YES price at (or shortly after) market open, if known.
    #[serde(default)]
    pub open_px: Option<Fp>,
}

impl MarketInfo {
    pub fn settled_outcome(&self) -> Option<Outcome> {
        match self.result.as_str() {
            "yes" => Some(Outcome::Yes),
            "no" => Some(Outcome::No),
            _ => None,
        }
    }
}

/// Reference price from an external feed (e.g. Coinbase BTC-USD, or the CF
/// Benchmarks BRTI index Kalshi settles against).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RefPrice {
    pub source: String,
    pub symbol: String,
    pub ts_ms: i64,
    pub px: f64,
    /// Official running 60-second average of the index, when the feed provides it
    /// (Kalshi's cfbenchmarks channel) — this *is* the settlement value in progress.
    #[serde(default)]
    pub avg_60s: Option<f64>,
}

/// One of *our* orders got (partially) filled on a live venue.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UserFill {
    pub venue: Venue,
    pub ticker: String,
    pub ts_ms: i64,
    pub trade_id: String,
    /// Venue-native order id.
    pub order_id: String,
    /// True = we bought YES / sold NO (net YES exposure up).
    pub buy_yes: bool,
    pub yes_px: Fp,
    pub qty: Fp,
    pub fee: Fp,
    pub is_taker: bool,
}

/// Every input a strategy can observe, from any venue, in one enum so that the
/// backtester and the live engine can drive strategies identically.
#[derive(Clone, Debug, PartialEq)]
pub enum MarketEvent {
    Market(MarketInfo),
    UserFill(UserFill),
    BookSnapshot {
        venue: Venue,
        ticker: String,
        ts_ms: i64,
        seq: i64,
        bids: Vec<(Fp, Fp)>,
        asks: Vec<(Fp, Fp)>,
    },
    /// Level quantity changed by `delta` (Kalshi style).
    BookDelta {
        venue: Venue,
        ticker: String,
        ts_ms: i64,
        seq: i64,
        side: BookSide,
        px: Fp,
        delta: Fp,
    },
    /// Level quantity set to `qty` (Polymarket style).
    BookLevel {
        venue: Venue,
        ticker: String,
        ts_ms: i64,
        side: BookSide,
        px: Fp,
        qty: Fp,
    },
    Trade(Trade),
    Ticker {
        venue: Venue,
        ticker: String,
        ts_ms: i64,
        yes_bid: Option<Fp>,
        yes_ask: Option<Fp>,
        last: Option<Fp>,
    },
    Ref(RefPrice),
    Settlement {
        venue: Venue,
        ticker: String,
        ts_ms: i64,
        result: Outcome,
    },
}

impl MarketEvent {
    pub fn ts_ms(&self) -> i64 {
        match self {
            MarketEvent::Market(m) => m.open_ts_ms,
            MarketEvent::BookSnapshot { ts_ms, .. }
            | MarketEvent::BookDelta { ts_ms, .. }
            | MarketEvent::BookLevel { ts_ms, .. }
            | MarketEvent::Ticker { ts_ms, .. }
            | MarketEvent::Settlement { ts_ms, .. } => *ts_ms,
            MarketEvent::Trade(t) => t.ts_ms,
            MarketEvent::Ref(r) => r.ts_ms,
            MarketEvent::UserFill(f) => f.ts_ms,
        }
    }

    pub fn ticker(&self) -> Option<&str> {
        match self {
            MarketEvent::Market(m) => Some(&m.ticker),
            MarketEvent::BookSnapshot { ticker, .. }
            | MarketEvent::BookDelta { ticker, .. }
            | MarketEvent::BookLevel { ticker, .. }
            | MarketEvent::Ticker { ticker, .. }
            | MarketEvent::Settlement { ticker, .. } => Some(ticker),
            MarketEvent::Trade(t) => Some(&t.ticker),
            MarketEvent::UserFill(f) => Some(&f.ticker),
            MarketEvent::Ref(_) => None,
        }
    }

    /// Ordering key so that same-timestamp events replay deterministically:
    /// market metadata first, then reference prices, then book/trade data, then settlement.
    pub fn kind_rank(&self) -> u8 {
        match self {
            MarketEvent::Market(_) => 0,
            MarketEvent::Ref(_) => 1,
            MarketEvent::BookSnapshot { .. } => 2,
            MarketEvent::BookDelta { .. } | MarketEvent::BookLevel { .. } => 3,
            MarketEvent::Ticker { .. } => 4,
            MarketEvent::Trade(_) => 5,
            MarketEvent::UserFill(_) => 6,
            MarketEvent::Settlement { .. } => 9,
        }
    }
}
