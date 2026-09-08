//! Flat, enum-free row types (so the Arrow schema is trivial and stable).
//! Prices/quantities are stored as i64 in 1/10000 units (see `mb_core::fp`).

use mb_core::{BookSide, Fp, MarketEvent, MarketInfo, Outcome, RefPrice, Trade, Venue};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TradeRow {
    pub venue: String,
    pub ticker: String,
    pub ts_ms: i64,
    pub yes_px: i64,
    pub qty: i64,
    pub taker_yes: bool,
    pub trade_id: String,
}

impl From<&Trade> for TradeRow {
    fn from(t: &Trade) -> Self {
        Self {
            venue: t.venue.as_str().into(),
            ticker: t.ticker.clone(),
            ts_ms: t.ts_ms,
            yes_px: t.yes_px.0,
            qty: t.qty.0,
            taker_yes: t.taker == Outcome::Yes,
            trade_id: t.trade_id.clone(),
        }
    }
}

impl TradeRow {
    pub fn to_trade(&self) -> Trade {
        Trade {
            venue: Venue::parse(&self.venue).unwrap_or(Venue::Kalshi),
            ticker: self.ticker.clone(),
            ts_ms: self.ts_ms,
            yes_px: Fp(self.yes_px),
            qty: Fp(self.qty),
            taker: if self.taker_yes { Outcome::Yes } else { Outcome::No },
            trade_id: self.trade_id.clone(),
        }
    }
}

/// One orderbook level change. `kind`: "snap" (absolute level from a snapshot,
/// with `seq` marking snapshot boundaries), "set" (absolute), "delta" (relative).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BookRow {
    pub venue: String,
    pub ticker: String,
    pub ts_ms: i64,
    pub seq: i64,
    pub kind: String,
    pub is_bid: bool,
    pub px: i64,
    pub qty: i64,
    /// True on the first row of a snapshot — the reader clears the book before applying.
    pub snapshot_start: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RefRow {
    pub source: String,
    pub symbol: String,
    pub ts_ms: i64,
    pub px: f64,
    #[serde(default)]
    pub avg_60s: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketRow {
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
    pub result: String,
    pub settlement_value: Option<f64>,
    pub volume: i64,
}

impl From<&MarketInfo> for MarketRow {
    fn from(m: &MarketInfo) -> Self {
        Self {
            venue: m.venue.clone(),
            ticker: m.ticker.clone(),
            event_ticker: m.event_ticker.clone(),
            series: m.series.clone(),
            title: m.title.clone(),
            strike_type: m.strike_type.clone(),
            floor_strike: m.floor_strike,
            cap_strike: m.cap_strike,
            open_ts_ms: m.open_ts_ms,
            close_ts_ms: m.close_ts_ms,
            expiration_ts_ms: m.expiration_ts_ms,
            status: m.status.clone(),
            result: m.result.clone(),
            settlement_value: m.settlement_value,
            volume: m.volume.0,
        }
    }
}

impl MarketRow {
    pub fn to_info(&self) -> MarketInfo {
        MarketInfo {
            venue: self.venue.clone(),
            ticker: self.ticker.clone(),
            event_ticker: self.event_ticker.clone(),
            series: self.series.clone(),
            title: self.title.clone(),
            strike_type: self.strike_type.clone(),
            floor_strike: self.floor_strike,
            cap_strike: self.cap_strike,
            open_ts_ms: self.open_ts_ms,
            close_ts_ms: self.close_ts_ms,
            expiration_ts_ms: self.expiration_ts_ms,
            status: self.status.clone(),
            result: self.result.clone(),
            settlement_value: self.settlement_value,
            yes_bid: None,
            yes_ask: None,
            volume: Fp(self.volume),
            category: String::new(),
            open_px: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CandleRow {
    pub source: String,
    pub symbol: String,
    /// bucket start, unix seconds
    pub ts: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TickerRow {
    pub venue: String,
    pub ticker: String,
    pub ts_ms: i64,
    pub yes_bid: Option<i64>,
    pub yes_ask: Option<i64>,
    pub last: Option<i64>,
}

/// Convert a live event into the row(s) it should be persisted as.
pub enum RowBatch {
    Trades(Vec<TradeRow>),
    Book(Vec<BookRow>),
    Refs(Vec<RefRow>),
    Markets(Vec<MarketRow>),
    Tickers(Vec<TickerRow>),
    Nothing,
}

pub fn event_to_rows(ev: &MarketEvent) -> RowBatch {
    match ev {
        MarketEvent::Trade(t) => RowBatch::Trades(vec![TradeRow::from(t)]),
        MarketEvent::Ref(r) => RowBatch::Refs(vec![RefRow {
            source: r.source.clone(),
            symbol: r.symbol.clone(),
            ts_ms: r.ts_ms,
            px: r.px,
            avg_60s: r.avg_60s,
        }]),
        MarketEvent::Market(m) => RowBatch::Markets(vec![MarketRow::from(m)]),
        MarketEvent::Ticker {
            venue,
            ticker,
            ts_ms,
            yes_bid,
            yes_ask,
            last,
        } => RowBatch::Tickers(vec![TickerRow {
            venue: venue.as_str().into(),
            ticker: ticker.clone(),
            ts_ms: *ts_ms,
            yes_bid: yes_bid.map(|p| p.0),
            yes_ask: yes_ask.map(|p| p.0),
            last: last.map(|p| p.0),
        }]),
        MarketEvent::BookSnapshot {
            venue,
            ticker,
            ts_ms,
            seq,
            bids,
            asks,
        } => {
            let mut rows = Vec::with_capacity(bids.len() + asks.len() + 1);
            let mk = |is_bid: bool, px: Fp, qty: Fp, first: bool| BookRow {
                venue: venue.as_str().into(),
                ticker: ticker.clone(),
                ts_ms: *ts_ms,
                seq: *seq,
                kind: "snap".into(),
                is_bid,
                px: px.0,
                qty: qty.0,
                snapshot_start: first,
            };
            // Always emit at least one row so an empty snapshot still clears the book.
            if bids.is_empty() && asks.is_empty() {
                rows.push(mk(true, Fp::ZERO, Fp::ZERO, true));
            }
            let mut first = true;
            for (p, q) in bids {
                rows.push(mk(true, *p, *q, first));
                first = false;
            }
            for (p, q) in asks {
                rows.push(mk(false, *p, *q, first));
                first = false;
            }
            RowBatch::Book(rows)
        }
        MarketEvent::BookDelta {
            venue,
            ticker,
            ts_ms,
            seq,
            side,
            px,
            delta,
        } => RowBatch::Book(vec![BookRow {
            venue: venue.as_str().into(),
            ticker: ticker.clone(),
            ts_ms: *ts_ms,
            seq: *seq,
            kind: "delta".into(),
            is_bid: *side == BookSide::Bid,
            px: px.0,
            qty: delta.0,
            snapshot_start: false,
        }]),
        MarketEvent::BookLevel {
            venue,
            ticker,
            ts_ms,
            side,
            px,
            qty,
        } => RowBatch::Book(vec![BookRow {
            venue: venue.as_str().into(),
            ticker: ticker.clone(),
            ts_ms: *ts_ms,
            seq: 0,
            kind: "set".into(),
            is_bid: *side == BookSide::Bid,
            px: px.0,
            qty: qty.0,
            snapshot_start: false,
        }]),
        MarketEvent::Settlement { .. } | MarketEvent::UserFill(_) => RowBatch::Nothing,
    }
}

/// Rebuild book events from rows (consecutive `snap` rows with the same
/// ticker/ts/seq are grouped back into one snapshot).
pub fn book_rows_to_events(rows: &[BookRow]) -> Vec<MarketEvent> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < rows.len() {
        let r = &rows[i];
        let venue = Venue::parse(&r.venue).unwrap_or(Venue::Kalshi);
        match r.kind.as_str() {
            "snap" => {
                let mut bids = Vec::new();
                let mut asks = Vec::new();
                let mut j = i;
                while j < rows.len()
                    && rows[j].kind == "snap"
                    && rows[j].ticker == r.ticker
                    && rows[j].seq == r.seq
                    && rows[j].ts_ms == r.ts_ms
                    && (j == i || !rows[j].snapshot_start)
                {
                    let x = &rows[j];
                    if x.qty > 0 {
                        if x.is_bid {
                            bids.push((Fp(x.px), Fp(x.qty)));
                        } else {
                            asks.push((Fp(x.px), Fp(x.qty)));
                        }
                    }
                    j += 1;
                }
                out.push(MarketEvent::BookSnapshot {
                    venue,
                    ticker: r.ticker.clone(),
                    ts_ms: r.ts_ms,
                    seq: r.seq,
                    bids,
                    asks,
                });
                i = j;
            }
            "delta" => {
                out.push(MarketEvent::BookDelta {
                    venue,
                    ticker: r.ticker.clone(),
                    ts_ms: r.ts_ms,
                    seq: r.seq,
                    side: if r.is_bid { BookSide::Bid } else { BookSide::Ask },
                    px: Fp(r.px),
                    delta: Fp(r.qty),
                });
                i += 1;
            }
            _ => {
                out.push(MarketEvent::BookLevel {
                    venue,
                    ticker: r.ticker.clone(),
                    ts_ms: r.ts_ms,
                    side: if r.is_bid { BookSide::Bid } else { BookSide::Ask },
                    px: Fp(r.px),
                    qty: Fp(r.qty),
                });
                i += 1;
            }
        }
    }
    out
}

pub fn ref_row_to_event(r: &RefRow) -> MarketEvent {
    MarketEvent::Ref(RefPrice {
        source: r.source.clone(),
        symbol: r.symbol.clone(),
        ts_ms: r.ts_ms,
        px: r.px,
        avg_60s: r.avg_60s,
    })
}
