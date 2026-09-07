//! Buffers live events and flushes them to hourly Parquet files:
//! `<root>/<kind>/<YYYY-MM-DD>/<HH>-<n>.parquet`.

use crate::parquet_io::write_parquet;
use crate::rows::*;
use anyhow::Result;
use mb_core::MarketEvent;
use std::path::PathBuf;
use std::time::Instant;
use tracing::info;

pub struct Recorder {
    root: PathBuf,
    trades: Vec<TradeRow>,
    books: Vec<BookRow>,
    refs: Vec<RefRow>,
    markets: Vec<MarketRow>,
    tickers: Vec<TickerRow>,
    last_flush: Instant,
    flush_every_secs: u64,
    max_rows: usize,
    seq: u64,
    pub rows_written: u64,
}

impl Recorder {
    pub fn new(root: impl Into<PathBuf>, flush_every_secs: u64, max_rows: usize) -> Self {
        Self {
            root: root.into(),
            trades: Vec::new(),
            books: Vec::new(),
            refs: Vec::new(),
            markets: Vec::new(),
            tickers: Vec::new(),
            last_flush: Instant::now(),
            flush_every_secs,
            max_rows,
            seq: 0,
            rows_written: 0,
        }
    }

    pub fn record(&mut self, ev: &MarketEvent) -> Result<()> {
        match event_to_rows(ev) {
            RowBatch::Trades(r) => self.trades.extend(r),
            RowBatch::Book(r) => self.books.extend(r),
            RowBatch::Refs(r) => self.refs.extend(r),
            RowBatch::Markets(r) => self.markets.extend(r),
            RowBatch::Tickers(r) => self.tickers.extend(r),
            RowBatch::Nothing => {}
        }
        let pending = self.trades.len() + self.books.len() + self.refs.len() + self.markets.len() + self.tickers.len();
        if pending >= self.max_rows || self.last_flush.elapsed().as_secs() >= self.flush_every_secs {
            self.flush()?;
        }
        Ok(())
    }

    fn path(&mut self, kind: &str) -> PathBuf {
        let now = chrono::Utc::now();
        self.seq += 1;
        self.root
            .join(kind)
            .join(now.format("%Y-%m-%d").to_string())
            .join(format!("{}-{:06}.parquet", now.format("%H"), self.seq))
    }

    pub fn flush(&mut self) -> Result<()> {
        macro_rules! flush_kind {
            ($field:ident, $kind:literal) => {
                if !self.$field.is_empty() {
                    let rows = std::mem::take(&mut self.$field);
                    let p = self.path($kind);
                    write_parquet(&p, &rows)?;
                    self.rows_written += rows.len() as u64;
                    info!(path = %p.display(), rows = rows.len(), "flushed");
                }
            };
        }
        flush_kind!(trades, "trades");
        flush_kind!(books, "books");
        flush_kind!(refs, "refs");
        flush_kind!(markets, "markets");
        flush_kind!(tickers, "tickers");
        self.last_flush = Instant::now();
        Ok(())
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
