//! Order-flow imbalance on short-dated markets: net aggressor volume
//! (YES-taker minus NO-taker contracts) over a rolling window predicts the
//! next move (follow) or is exhausted and reverts (fade). Trades as a taker
//! when |imbalance| exceeds a threshold, holds to settlement. Runs on the
//! trade tape in backtests and on live trades in paper.

use anyhow::{Context as _, Result};
use mb_core::{Context, Fp, MarketEvent, OrderRequest, Strategy, Tif};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FlowConfig {
    pub series: String,
    /// Window over which signed taker flow is summed, seconds.
    pub window_secs: i64,
    /// Trigger when |net flow| / total flow ≥ this (0..1) and total flow ≥ min_volume.
    pub imbalance: f64,
    pub min_volume: f64,
    /// true = trade in the direction of the flow; false = fade it.
    pub follow: bool,
    /// Dollars per entry; entries per market capped.
    pub stake: f64,
    pub max_entries_per_market: u32,
    /// Don't trade with less than this many seconds to close.
    pub min_tau_secs: i64,
    /// Don't trade when the price is outside this band.
    pub min_price: f64,
    pub max_price: f64,
    pub cooldown_ms: i64,
}

impl Default for FlowConfig {
    fn default() -> Self {
        Self {
            series: "KXBTC15M".into(),
            window_secs: 60,
            imbalance: 0.6,
            min_volume: 200.0,
            follow: true,
            stake: 2.0,
            max_entries_per_market: 2,
            min_tau_secs: 120,
            min_price: 0.15,
            max_price: 0.85,
            cooldown_ms: 5_000,
        }
    }
}

impl FlowConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let s = std::fs::read_to_string(path.as_ref()).with_context(|| format!("reading {}", path.as_ref().display()))?;
        toml::from_str(&s).context("parsing flow config")
    }
}

struct Mkt {
    close_ts_ms: i64,
    flows: VecDeque<(i64, f64)>, // (ts, signed qty)
    entries: u32,
    last_order_ms: i64,
}

pub struct FlowStrategy {
    cfg: FlowConfig,
    mkts: HashMap<String, Mkt>,
    pub orders: u64,
    pub signals: u64,
}

impl FlowStrategy {
    pub fn new(cfg: FlowConfig) -> Self {
        Self {
            cfg,
            mkts: HashMap::new(),
            orders: 0,
            signals: 0,
        }
    }

    fn evaluate(&mut self, ticker: &str, ctx: &mut dyn Context) {
        let now = ctx.now_ms();
        let Some(m) = self.mkts.get_mut(ticker) else { return };
        while m.flows.front().map(|(t, _)| now - *t > self.cfg.window_secs * 1000).unwrap_or(false) {
            m.flows.pop_front();
        }
        if m.entries >= self.cfg.max_entries_per_market || now - m.last_order_ms < self.cfg.cooldown_ms {
            return;
        }
        if (m.close_ts_ms - now) / 1000 < self.cfg.min_tau_secs {
            return;
        }
        let net: f64 = m.flows.iter().map(|(_, q)| *q).sum();
        let total: f64 = m.flows.iter().map(|(_, q)| q.abs()).sum();
        if total < self.cfg.min_volume || net.abs() / total < self.cfg.imbalance {
            return;
        }
        let Some(book) = ctx.book(ticker) else { return };
        let (Some((bid, bq)), Some((ask, aq))) = (book.best_bid(), book.best_ask()) else { return };
        let mid = (bid.to_f64() + ask.to_f64()) / 2.0;
        if mid < self.cfg.min_price || mid > self.cfg.max_price {
            return;
        }
        self.signals += 1;
        let buyers_dominate = net > 0.0;
        let buy_yes = if self.cfg.follow { buyers_dominate } else { !buyers_dominate };
        let (mut req, px, avail) = if buy_yes {
            (OrderRequest::buy_yes(ticker, ask, Fp::ZERO, Tif::Ioc), ask.to_f64(), aq.to_f64())
        } else {
            (OrderRequest::sell_yes(ticker, bid, Fp::ZERO, Tif::Ioc), 1.0 - bid.to_f64(), bq.to_f64())
        };
        let qty = (self.cfg.stake / px.max(0.02)).floor().min(avail).max(0.0);
        if qty < 1.0 {
            return;
        }
        req.qty = Fp::from_int(qty as i64);
        req.tag = if self.cfg.follow { "flow_follow" } else { "flow_fade" };
        ctx.submit(req);
        m.entries += 1;
        m.last_order_ms = now;
        self.orders += 1;
    }
}

impl Strategy for FlowStrategy {
    fn name(&self) -> &str {
        if self.cfg.follow { "flow_follow" } else { "flow_fade" }
    }
    fn on_event(&mut self, ev: &MarketEvent, ctx: &mut dyn Context) {
        match ev {
            MarketEvent::Market(m) => {
                if m.series == self.cfg.series {
                    self.mkts.entry(m.ticker.clone()).or_insert(Mkt {
                        close_ts_ms: m.close_ts_ms,
                        flows: VecDeque::new(),
                        entries: 0,
                        last_order_ms: 0,
                    });
                }
            }
            MarketEvent::Trade(t) => {
                if let Some(m) = self.mkts.get_mut(&t.ticker) {
                    let signed = if t.taker == mb_core::Outcome::Yes { t.qty.to_f64() } else { -t.qty.to_f64() };
                    m.flows.push_back((t.ts_ms, signed));
                    let tk = t.ticker.clone();
                    self.evaluate(&tk, ctx);
                }
            }
            MarketEvent::Settlement { ticker, .. } => {
                self.mkts.remove(ticker);
            }
            _ => {}
        }
    }
    fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"kind": "flow", "mode": "taker", "follow": self.cfg.follow, "window_secs": self.cfg.window_secs, "imbalance": self.cfg.imbalance,
                           "series": [self.cfg.series], "orders": self.orders, "signals": self.signals,
                           "markets": self.mkts.iter().map(|(t, m)| serde_json::json!({"ticker": t, "close_ts_ms": m.close_ts_ms, "net_flow": m.flows.iter().map(|(_, q)| q).sum::<f64>()})).collect::<Vec<_>>()})
    }
}
