//! Model-free thin-book market maker.
//!
//! Hypothesis under test: in slow, retail-driven markets (daily weather buckets)
//! the spread is wide relative to price and adverse selection is slow, so a
//! liquidity provider quoting around the mid with inventory skew earns the spread.
//! fair = mid − skew·inventory; bid = fair − half_spread, ask = fair + half_spread,
//! post-only on the tapered tick grid, never crossing the touch, inventory capped.

use crate::btc15m::kalshi_tick;
use anyhow::{Context as _, Result};
use mb_core::{Context, Fill, Fp, MarketEvent, OrderId, OrderRequest, Strategy, Tif};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SpreadMakerConfig {
    /// Series to quote (empty = every market we are told about).
    pub series: Vec<String>,
    /// Half the quoted spread around fair, in dollars.
    pub half_spread: f64,
    /// Contracts per quote.
    pub quote_qty: f64,
    /// |inventory| cap per market (contracts).
    pub max_inventory: f64,
    /// Fair-value shift per contract of inventory (dollars). Long → quote lower.
    pub skew_per_contract: f64,
    /// Stop quoting this many seconds before close.
    pub min_tau_secs: i64,
    /// Don't quote when the market spread is wider than this (illiquid/one-sided).
    pub max_market_spread: f64,
    /// Don't quote outside this price band (tails are where retail is right and we're wrong).
    pub min_price: f64,
    pub max_price: f64,
    /// Minimum ms between requotes per market.
    pub requote_ms: i64,
    /// Max simultaneous markets quoted.
    pub max_markets: usize,
    /// Only quote when the market closes within this many seconds (0 = no limit).
    /// The favourite-maker edge lives in the final minutes, where the outcome is
    /// nearly settled and the spread is the whole return.
    pub max_tau_secs: i64,
    /// Quote only the bid (buy the favourite), never the offer. Taking the other side
    /// of a near-certain favourite is how a maker gets run over.
    pub bid_only: bool,
}

impl Default for SpreadMakerConfig {
    fn default() -> Self {
        Self {
            series: Vec::new(),
            half_spread: 0.01,
            quote_qty: 5.0,
            max_inventory: 25.0,
            skew_per_contract: 0.001,
            min_tau_secs: 1800,
            max_market_spread: 0.10,
            min_price: 0.05,
            max_price: 0.95,
            requote_ms: 2000,
            max_markets: 200,
            max_tau_secs: 0,
            bid_only: false,
        }
    }
}

impl SpreadMakerConfig {
    /// Cap per-market inventory at ~2 % of the bankroll (worst case $0.50/contract).
    pub fn scale_to_bankroll(&mut self, bankroll: f64) {
        let contracts = ((0.02 * bankroll) / 0.5).floor().max(2.0);
        self.max_inventory = self.max_inventory.min(contracts);
        self.quote_qty = self.quote_qty.min(self.max_inventory);
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let s = std::fs::read_to_string(path.as_ref()).with_context(|| format!("reading {}", path.as_ref().display()))?;
        toml::from_str(&s).context("parsing spread maker config")
    }
}

#[derive(Debug, Clone)]
struct Mkt {
    series: String,
    close_ts_ms: i64,
    bid_order: Option<(OrderId, Fp)>,
    ask_order: Option<(OrderId, Fp)>,
    last_quote_ms: i64,
    last_mid: Option<f64>,
    last_fair: Option<f64>,
}

pub struct SpreadMaker {
    cfg: SpreadMakerConfig,
    mkts: HashMap<String, Mkt>,
    pub orders: u64,
}

impl SpreadMaker {
    pub fn new(cfg: SpreadMakerConfig) -> Self {
        Self {
            cfg,
            mkts: HashMap::new(),
            orders: 0,
        }
    }

    fn cancel_all(&mut self, ticker: &str, ctx: &mut dyn Context) {
        if let Some(m) = self.mkts.get_mut(ticker) {
            if let Some((id, _)) = m.bid_order.take() {
                ctx.cancel(id);
            }
            if let Some((id, _)) = m.ask_order.take() {
                ctx.cancel(id);
            }
        }
    }

    fn quote(&mut self, ticker: &str, ctx: &mut dyn Context) {
        let now = ctx.now_ms();
        let Some(m) = self.mkts.get(ticker).cloned() else { return };
        let secs_left = if m.close_ts_ms > 0 { (m.close_ts_ms - now) / 1000 } else { i64::MAX };
        if secs_left < self.cfg.min_tau_secs || (self.cfg.max_tau_secs > 0 && secs_left > self.cfg.max_tau_secs) {
            self.cancel_all(ticker, ctx);
            return;
        }
        if now - m.last_quote_ms < self.cfg.requote_ms {
            return;
        }
        let (bb, ba) = match ctx.book(ticker) {
            Some(b) => (b.best_bid(), b.best_ask()),
            None => (None, None),
        };
        let (Some((bid, _)), Some((ask, _))) = (bb, ba) else {
            self.cancel_all(ticker, ctx);
            return;
        };
        let mid = (bid.to_f64() + ask.to_f64()) / 2.0;
        let spread = ask.to_f64() - bid.to_f64();
        if spread > self.cfg.max_market_spread || mid < self.cfg.min_price || mid > self.cfg.max_price {
            self.cancel_all(ticker, ctx);
            return;
        }
        let inv = ctx.position(ticker).yes_qty.to_f64();
        let fair = mid - self.cfg.skew_per_contract * inv;
        // skip if nothing moved
        if m.last_mid == Some(mid) && m.last_fair == Some(fair) && (m.bid_order.is_some() || m.ask_order.is_some()) {
            return;
        }

        let mut want_bid = None;
        if inv < self.cfg.max_inventory {
            let raw = Fp::from_f64(fair - self.cfg.half_spread);
            let mut px = raw.round_down_to(kalshi_tick(raw));
            if px >= ask {
                px = ask - kalshi_tick(ask);
            }
            if px.to_f64() >= self.cfg.min_price {
                want_bid = Some(px);
            }
        }
        let mut want_ask = None;
        if !self.cfg.bid_only && inv > -self.cfg.max_inventory {
            let raw = Fp::from_f64(fair + self.cfg.half_spread);
            let mut px = raw.round_up_to(kalshi_tick(raw));
            if px <= bid {
                px = bid + kalshi_tick(bid);
            }
            if px.to_f64() <= self.cfg.max_price {
                want_ask = Some(px);
            }
        }
        if let (Some(b), Some(a)) = (want_bid, want_ask)
            && b >= a
        {
            want_ask = None;
        }

        let qty = Fp::from_f64(self.cfg.quote_qty);
        let Some(mm) = self.mkts.get_mut(ticker) else { return };
        mm.last_quote_ms = now;
        mm.last_mid = Some(mid);
        mm.last_fair = Some(fair);
        match (mm.bid_order, want_bid) {
            (Some((_, p)), Some(w)) if p == w => {}
            (cur, w) => {
                if let Some((id, _)) = cur {
                    ctx.cancel(id);
                    mm.bid_order = None;
                }
                if let Some(w) = w {
                    let id = ctx.submit(OrderRequest::buy_yes(ticker, w, qty, Tif::Gtc).post_only().tagged("sm_bid"));
                    mm.bid_order = Some((id, w));
                    self.orders += 1;
                }
            }
        }
        match (mm.ask_order, want_ask) {
            (Some((_, p)), Some(w)) if p == w => {}
            (cur, w) => {
                if let Some((id, _)) = cur {
                    ctx.cancel(id);
                    mm.ask_order = None;
                }
                if let Some(w) = w {
                    let id = ctx.submit(OrderRequest::sell_yes(ticker, w, qty, Tif::Gtc).post_only().tagged("sm_ask"));
                    mm.ask_order = Some((id, w));
                    self.orders += 1;
                }
            }
        }
    }
}

impl Strategy for SpreadMaker {
    fn name(&self) -> &str {
        "spread_maker"
    }

    fn on_event(&mut self, ev: &MarketEvent, ctx: &mut dyn Context) {
        match ev {
            MarketEvent::Market(m) => {
                if !self.cfg.series.is_empty() && !self.cfg.series.contains(&m.series) {
                    return;
                }
                if self.mkts.len() >= self.cfg.max_markets && !self.mkts.contains_key(&m.ticker) {
                    return;
                }
                let e = self.mkts.entry(m.ticker.clone()).or_insert(Mkt {
                    series: m.series.clone(),
                    close_ts_ms: m.close_ts_ms,
                    bid_order: None,
                    ask_order: None,
                    last_quote_ms: 0,
                    last_mid: None,
                    last_fair: None,
                });
                e.close_ts_ms = m.close_ts_ms;
            }
            MarketEvent::BookSnapshot { ticker, .. }
            | MarketEvent::BookDelta { ticker, .. }
            | MarketEvent::BookLevel { ticker, .. }
            | MarketEvent::Ticker { ticker, .. } => {
                if self.mkts.contains_key(ticker) {
                    let t = ticker.clone();
                    self.quote(&t, ctx);
                }
            }
            MarketEvent::Trade(t) => {
                if self.mkts.contains_key(&t.ticker) {
                    let tk = t.ticker.clone();
                    self.quote(&tk, ctx);
                }
            }
            MarketEvent::Settlement { ticker, .. } => {
                self.mkts.remove(ticker);
            }
            MarketEvent::Ref(_) | MarketEvent::UserFill(_) => {}
        }
    }

    fn on_fill(&mut self, fill: &Fill, ctx: &mut dyn Context) {
        // a fully-filled quote is gone from the book; forget it so we re-quote
        if let Some(m) = self.mkts.get_mut(&fill.ticker) {
            let still_open: Vec<OrderId> = ctx.open_orders(&fill.ticker).into_iter().map(|(id, _)| id).collect();
            if m.bid_order.is_some_and(|(id, _)| !still_open.contains(&id)) {
                m.bid_order = None;
            }
            if m.ask_order.is_some_and(|(id, _)| !still_open.contains(&id)) {
                m.ask_order = None;
            }
            m.last_quote_ms = 0; // requote immediately with the new inventory
        }
    }

    fn snapshot(&self) -> serde_json::Value {
        let markets: Vec<serde_json::Value> = self
            .mkts
            .iter()
            .map(|(t, m)| {
                serde_json::json!({
                    "ticker": t,
                    "series": m.series,
                    "close_ts_ms": m.close_ts_ms,
                    "mid": m.last_mid,
                    "fair": m.last_fair,
                    "bid_quote": m.bid_order.map(|(_, p)| p.to_f64()),
                    "ask_quote": m.ask_order.map(|(_, p)| p.to_f64()),
                })
            })
            .collect();
        serde_json::json!({
            "kind": "spread_maker",
            "mode": "maker",
            "series": self.cfg.series,
            "half_spread": self.cfg.half_spread,
            "quote_qty": self.cfg.quote_qty,
            "max_inventory": self.cfg.max_inventory,
            "orders": self.orders,
            "markets": markets,
        })
    }
}
