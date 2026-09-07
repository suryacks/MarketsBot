//! Build a replayable, time-ordered event stream from the Parquet data dir.
//!
//! Layout written by `mbot fetch-history` / `mbot collect`:
//!   <root>/markets/**.parquet   MarketRow
//!   <root>/trades/**.parquet    TradeRow
//!   <root>/candles/**.parquet   CandleRow   (historical ref prices)
//!   <root>/refs/**.parquet      RefRow      (live-recorded ref prices)
//!   <root>/books/**.parquet     BookRow     (live-recorded books)

use anyhow::Result;
use mb_core::{MarketEvent, Outcome, RefPrice, Venue};
use mb_data::{book_rows_to_events, read_dir, ref_row_to_event, BookRow, CandleRow, MarketRow, RefRow, TradeRow};
use std::collections::HashSet;
use std::path::Path;
use tracing::info;

pub struct HistoryFilter {
    pub series: Option<String>,
    pub from_ms: i64,
    pub to_ms: i64,
    pub ref_symbol: Option<String>,
}

pub fn load_events(root: &Path, f: &HistoryFilter) -> Result<Vec<MarketEvent>> {
    let mut events: Vec<MarketEvent> = Vec::new();

    // markets + settlements
    let markets: Vec<MarketRow> = read_dir(root.join("markets"))?;
    let mut seen = HashSet::new();
    let mut tickers = HashSet::new();
    for m in markets.iter() {
        if let Some(s) = &f.series
            && &m.series != s
        {
            continue;
        }
        if m.close_ts_ms < f.from_ms || m.open_ts_ms > f.to_ms || !seen.insert(m.ticker.clone()) {
            continue;
        }
        tickers.insert(m.ticker.clone());
        let info = m.to_info();
        let result = info.settled_outcome();
        events.push(MarketEvent::Market(info));
        if let Some(r) = result {
            events.push(MarketEvent::Settlement {
                venue: Venue::parse(&m.venue).unwrap_or(Venue::Kalshi),
                ticker: m.ticker.clone(),
                ts_ms: m.close_ts_ms,
                result: r,
            });
        }
    }
    let n_markets = tickers.len();

    // trades
    let trades: Vec<TradeRow> = read_dir(root.join("trades"))?;
    let mut n_trades = 0usize;
    for t in trades.iter() {
        if t.ts_ms < f.from_ms || t.ts_ms > f.to_ms || !tickers.contains(&t.ticker) {
            continue;
        }
        events.push(MarketEvent::Trade(t.to_trade()));
        n_trades += 1;
    }

    // reference prices: candles (2 points per bar) and/or live refs
    let candles: Vec<CandleRow> = read_dir(root.join("candles"))?;
    let mut n_ref = 0usize;
    for c in candles.iter() {
        if let Some(s) = &f.ref_symbol
            && &c.symbol != s
        {
            continue;
        }
        let t0 = c.ts * 1000;
        if t0 < f.from_ms - 3_600_000 || t0 > f.to_ms {
            continue;
        }
        events.push(MarketEvent::Ref(RefPrice {
            source: c.source.clone(),
            symbol: c.symbol.clone(),
            ts_ms: t0,
            px: c.open,
        }));
        events.push(MarketEvent::Ref(RefPrice {
            source: c.source.clone(),
            symbol: c.symbol.clone(),
            ts_ms: t0 + 59_000,
            px: c.close,
        }));
        n_ref += 2;
    }
    let refs: Vec<RefRow> = read_dir(root.join("refs"))?;
    for r in refs.iter() {
        if let Some(s) = &f.ref_symbol
            && &r.symbol != s
        {
            continue;
        }
        if r.ts_ms < f.from_ms || r.ts_ms > f.to_ms {
            continue;
        }
        events.push(ref_row_to_event(r));
        n_ref += 1;
    }

    // recorded books
    let books: Vec<BookRow> = read_dir(root.join("books"))?;
    let books: Vec<BookRow> = books
        .into_iter()
        .filter(|b| b.ts_ms >= f.from_ms && b.ts_ms <= f.to_ms && tickers.contains(&b.ticker))
        .collect();
    let n_books = books.len();
    events.extend(book_rows_to_events(&books));

    events.sort_by_key(|e| (e.ts_ms(), e.kind_rank()));
    info!(n_markets, n_trades, n_ref, n_books, total = events.len(), "loaded history");
    Ok(events)
}

pub fn outcome_str(o: Outcome) -> &'static str {
    o.as_str()
}
