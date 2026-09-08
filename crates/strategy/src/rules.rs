//! RuleTrader: executes mechanical rules discovered by `mbot universe` — as IOC
//! orders for a fixed dollar stake, held to settlement. One trade per
//! (rule, market). This is how a universe PASS gets its paper test (and, with
//! `mbot live`, its $100 test) without new code.
//!
//! Rule kinds (matching universe families):
//! * `bucket` — at horizon H before close, price in [lo, hi) → buy side
//! * `drift`  — at horizon H, price moved ≥ threshold since market open → buy side

use anyhow::{Context as _, Result};
use mb_core::{Context, Fp, MarketEvent, OrderRequest, Strategy, Tif};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rule {
    /// "ALL", "CAT:<category>" or a series ticker
    pub scope: String,
    pub horizon_secs: i64,
    /// "bucket" | "drift"
    #[serde(default = "default_kind")]
    pub kind: String,
    #[serde(default)]
    pub px_lo: f64,
    #[serde(default = "one")]
    pub px_hi: f64,
    /// drift: signed threshold (+0.10 = up at least 10¢ since open; −0.10 = down at least 10¢)
    #[serde(default)]
    pub drift: f64,
    /// "BuyYes" | "BuyNo"
    pub side: String,
    #[serde(default)]
    pub label: String,
}

fn default_kind() -> String {
    "bucket".into()
}
fn one() -> f64 {
    1.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RuleTraderConfig {
    pub rules: Vec<Rule>,
    /// Dollars per trade.
    pub stake: f64,
    /// Max simultaneous open positions.
    pub max_positions: usize,
    /// How far around the horizon a market is eligible (fraction of horizon, min 60 s).
    pub window_frac: f64,
}

impl Default for RuleTraderConfig {
    fn default() -> Self {
        Self {
            rules: Vec::new(),
            stake: 2.0,
            max_positions: 40,
            window_frac: 0.15,
        }
    }
}

fn nums(s: &str) -> Vec<f64> {
    s.split(|c: char| !c.is_ascii_digit() && c != '.' && c != '-' && c != '+')
        .filter_map(|x| x.parse().ok())
        .collect()
}

impl RuleTraderConfig {
    /// Load rules from a universe.json (rows with the given verdict) or a plain rules JSON array.
    pub fn from_json(path: impl AsRef<Path>, verdict: &str, min_t: f64, stake: f64) -> Result<Self> {
        let v: serde_json::Value = serde_json::from_slice(&std::fs::read(path.as_ref()).with_context(|| format!("reading {}", path.as_ref().display()))?)?;
        let mut rules = Vec::new();
        if let Some(rows) = v.get("rows").and_then(|r| r.as_array()) {
            for r in rows {
                if r["verdict"].as_str() != Some(verdict) || r["oos"]["t"].as_f64().unwrap_or(0.0) < min_t {
                    continue;
                }
                let family = r["family"].as_str().unwrap_or("");
                let p = r["params"].as_str().unwrap_or("");
                let base = Rule {
                    scope: r["scope"].as_str().unwrap_or("ALL").to_string(),
                    horizon_secs: r["horizon"].as_i64().unwrap_or(3600),
                    kind: "bucket".into(),
                    px_lo: 0.0,
                    px_hi: 1.0,
                    drift: 0.0,
                    side: r["side"].as_str().unwrap_or("BuyYes").to_string(),
                    label: format!("{family} {} {p}", r["scope"].as_str().unwrap_or("")),
                };
                match family {
                    "price-bucket" => {
                        let n = nums(p);
                        if n.len() >= 2 {
                            rules.push(Rule { px_lo: n[0], px_hi: n[1], ..base });
                        }
                    }
                    "time-filter" => {
                        // "US evening 20-04 UTC, px 0.95-1.00" → the price part is the last two numbers;
                        // the time filter itself is not enforced live yet (documented approximation).
                        let n = nums(p);
                        if n.len() >= 2 {
                            rules.push(Rule { px_lo: n[n.len() - 2], px_hi: n[n.len() - 1], ..base });
                        }
                    }
                    "drift-since-open" => {
                        let n = nums(p);
                        if let Some(d) = n.first() {
                            rules.push(Rule { kind: "drift".into(), drift: *d, ..base });
                        }
                    }
                    "side-bias" => rules.push(base),
                    _ => {}
                }
            }
        } else if let Some(arr) = v.as_array() {
            rules = serde_json::from_value(serde_json::Value::Array(arr.clone()))?;
        }
        Ok(Self {
            rules,
            stake,
            ..Default::default()
        })
    }
}

#[derive(Clone, Debug)]
struct Mkt {
    series: String,
    category: String,
    close_ts_ms: i64,
    open_px: Option<f64>,
}

pub struct RuleTrader {
    cfg: RuleTraderConfig,
    markets: HashMap<String, Mkt>,
    traded: HashSet<(usize, String)>,
    positions_open: usize,
    pub orders: u64,
    pub evaluations: u64,
}

impl RuleTrader {
    pub fn new(cfg: RuleTraderConfig) -> Self {
        Self {
            cfg,
            markets: HashMap::new(),
            traded: HashSet::new(),
            positions_open: 0,
            orders: 0,
            evaluations: 0,
        }
    }

    fn applies(rule: &Rule, m: &Mkt) -> bool {
        rule.scope == "ALL" || rule.scope == m.series || rule.scope.strip_prefix("CAT:") == Some(m.category.as_str())
    }

    fn evaluate(&mut self, ticker: &str, ctx: &mut dyn Context) {
        let Some(m) = self.markets.get(ticker).cloned() else { return };
        let now = ctx.now_ms();
        let secs_left = (m.close_ts_ms - now) as f64 / 1000.0;
        if secs_left <= 0.0 || self.positions_open >= self.cfg.max_positions {
            return;
        }
        let Some(book) = ctx.book(ticker) else { return };
        let (Some((bid, bq)), Some((ask, aq))) = (book.best_bid(), book.best_ask()) else { return };
        let mid = (bid.to_f64() + ask.to_f64()) / 2.0;
        if ask.to_f64() - bid.to_f64() > 0.10 {
            return; // one-sided / illiquid
        }
        self.evaluations += 1;
        let mut to_submit = Vec::new();
        for (i, rule) in self.cfg.rules.iter().enumerate() {
            if self.traded.contains(&(i, ticker.to_string())) || !Self::applies(rule, &m) {
                continue;
            }
            let h = rule.horizon_secs as f64;
            let window = (h * self.cfg.window_frac).max(60.0);
            if (secs_left - h).abs() > window {
                continue;
            }
            let hit = match rule.kind.as_str() {
                "drift" => match m.open_px {
                    Some(o) => {
                        let d = mid - o;
                        if rule.drift >= 0.0 { d >= rule.drift } else { d <= rule.drift }
                    }
                    None => false,
                },
                _ => mid >= rule.px_lo && mid < rule.px_hi,
            };
            if !hit || ctx.position(ticker).yes_qty.abs().to_f64() > 0.0 {
                continue;
            }
            let (mut req, px, avail) = if rule.side == "BuyYes" {
                (OrderRequest::buy_yes(ticker, ask, Fp::ZERO, Tif::Ioc), ask.to_f64(), aq.to_f64())
            } else {
                (OrderRequest::sell_yes(ticker, bid, Fp::ZERO, Tif::Ioc), 1.0 - bid.to_f64(), bq.to_f64())
            };
            let qty = (self.cfg.stake / px.max(0.02)).floor().max(1.0).min(avail);
            if qty < 1.0 {
                continue;
            }
            req.qty = Fp::from_int(qty as i64);
            req.tag = if rule.side == "BuyYes" { "rule_buy_yes" } else { "rule_buy_no" };
            to_submit.push((i, req));
        }
        for (i, req) in to_submit {
            self.traded.insert((i, ticker.to_string()));
            ctx.submit(req);
            self.orders += 1;
            self.positions_open += 1;
        }
    }
}

impl Strategy for RuleTrader {
    fn name(&self) -> &str {
        "rule_trader"
    }
    fn on_event(&mut self, ev: &MarketEvent, ctx: &mut dyn Context) {
        match ev {
            MarketEvent::Market(m) => {
                let e = self.markets.entry(m.ticker.clone()).or_insert(Mkt {
                    series: m.series.clone(),
                    category: m.category.clone(),
                    close_ts_ms: m.close_ts_ms,
                    open_px: m.open_px.map(|p| p.to_f64()),
                });
                e.close_ts_ms = m.close_ts_ms;
                if !m.category.is_empty() {
                    e.category = m.category.clone();
                }
                if let Some(p) = m.open_px {
                    e.open_px = Some(p.to_f64());
                }
            }
            MarketEvent::BookSnapshot { ticker, .. } | MarketEvent::BookDelta { ticker, .. } | MarketEvent::BookLevel { ticker, .. } | MarketEvent::Ticker { ticker, .. } => {
                if self.markets.contains_key(ticker) {
                    let t = ticker.clone();
                    self.evaluate(&t, ctx);
                }
            }
            MarketEvent::Settlement { ticker, .. } => {
                if self.markets.remove(ticker).is_some() && self.traded.iter().any(|(_, t)| t == ticker) {
                    self.positions_open = self.positions_open.saturating_sub(1);
                }
            }
            _ => {}
        }
    }
    fn snapshot(&self) -> serde_json::Value {
        let mut per_scope: HashMap<&str, usize> = HashMap::new();
        for r in &self.cfg.rules {
            *per_scope.entry(r.scope.as_str()).or_default() += 1;
        }
        serde_json::json!({
            "kind": "rule_trader", "mode": "taker", "rules": self.cfg.rules.len(), "stake": self.cfg.stake, "orders": self.orders,
            "evaluations": self.evaluations, "markets_watched": self.markets.len(),
            "series": per_scope.keys().map(|s| s.to_string()).collect::<Vec<_>>(),
            "rule_labels": self.cfg.rules.iter().map(|r| r.label.clone()).collect::<Vec<_>>(),
            "markets": [],
        })
    }
}
