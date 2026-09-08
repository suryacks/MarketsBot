//! RuleTrader: executes mechanical rules discovered by `mbot universe` /
//! `mbot scan-bias` — "in scope S, at horizon H before close, when the price is
//! in [lo, hi), buy YES/NO" — as IOC orders for a fixed dollar stake, held to
//! settlement. One trade per (rule, market). This is how a universe PASS gets
//! its paper test (and, with `mbot live`, its $100 test) without new code.

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
    pub px_lo: f64,
    pub px_hi: f64,
    /// "BuyYes" | "BuyNo"
    pub side: String,
    #[serde(default)]
    pub label: String,
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

impl RuleTraderConfig {
    /// Load rules from a universe.json (rows with the given verdict) or a plain rules JSON array.
    pub fn from_json(path: impl AsRef<Path>, verdict: &str, min_t: f64, stake: f64) -> Result<Self> {
        let v: serde_json::Value = serde_json::from_slice(&std::fs::read(path.as_ref()).with_context(|| format!("reading {}", path.as_ref().display()))?)?;
        let mut rules = Vec::new();
        if let Some(rows) = v.get("rows").and_then(|r| r.as_array()) {
            for r in rows {
                if r["verdict"].as_str() != Some(verdict) || r["family"].as_str() != Some("price-bucket") {
                    continue;
                }
                if r["oos"]["t"].as_f64().unwrap_or(0.0) < min_t {
                    continue;
                }
                let p = r["params"].as_str().unwrap_or("");
                let nums: Vec<f64> = p.split(|c: char| !c.is_ascii_digit() && c != '.').filter_map(|x| x.parse().ok()).collect();
                if nums.len() < 2 {
                    continue;
                }
                rules.push(Rule {
                    scope: r["scope"].as_str().unwrap_or("ALL").to_string(),
                    horizon_secs: r["horizon"].as_i64().unwrap_or(3600),
                    px_lo: nums[0],
                    px_hi: nums[1],
                    side: r["side"].as_str().unwrap_or("BuyYes").to_string(),
                    label: format!("{} {} {}", r["family"].as_str().unwrap_or(""), r["scope"].as_str().unwrap_or(""), p),
                });
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

pub struct RuleTrader {
    cfg: RuleTraderConfig,
    /// ticker -> (series, category, close_ts_ms)
    markets: HashMap<String, (String, String, i64)>,
    traded: HashSet<(usize, String)>,
    pub orders: u64,
}

impl RuleTrader {
    pub fn new(cfg: RuleTraderConfig) -> Self {
        Self {
            cfg,
            markets: HashMap::new(),
            traded: HashSet::new(),
            orders: 0,
        }
    }

    fn rule_applies(rule: &Rule, series: &str, category: &str) -> bool {
        rule.scope == "ALL" || rule.scope == series || rule.scope.strip_prefix("CAT:") == Some(category)
    }

    fn evaluate(&mut self, ticker: &str, ctx: &mut dyn Context) {
        let Some((series, category, close_ms)) = self.markets.get(ticker).cloned() else { return };
        let now = ctx.now_ms();
        let secs_left = (close_ms - now) as f64 / 1000.0;
        if secs_left <= 0.0 {
            return;
        }
        let open_positions = ctx.open_orders("").len(); // not per-ticker: cheap proxy below
        let _ = open_positions;
        let Some(book) = ctx.book(ticker) else { return };
        let (Some((bid, bq)), Some((ask, aq))) = (book.best_bid(), book.best_ask()) else { return };
        let mid = (bid.to_f64() + ask.to_f64()) / 2.0;
        let mut to_submit = Vec::new();
        for (i, rule) in self.cfg.rules.iter().enumerate() {
            if self.traded.contains(&(i, ticker.to_string())) || !Self::rule_applies(rule, &series, &category) {
                continue;
            }
            let h = rule.horizon_secs as f64;
            let window = (h * self.cfg.window_frac).max(60.0);
            if (secs_left - h).abs() > window || mid < rule.px_lo || mid >= rule.px_hi {
                continue;
            }
            if ctx.position(ticker).yes_qty.abs().to_f64() > 0.0 {
                continue; // one position per market
            }
            let (req, px) = if rule.side == "BuyYes" {
                (OrderRequest::buy_yes(ticker, ask, Fp::ZERO, Tif::Ioc), ask.to_f64())
            } else {
                (OrderRequest::sell_yes(ticker, bid, Fp::ZERO, Tif::Ioc), 1.0 - bid.to_f64())
            };
            let qty = (self.cfg.stake / px.max(0.02)).floor().max(1.0).min(if rule.side == "BuyYes" { aq.to_f64() } else { bq.to_f64() });
            if qty < 1.0 {
                continue;
            }
            let mut req = req;
            req.qty = Fp::from_int(qty as i64);
            req.tag = if rule.side == "BuyYes" { "rule_buy_yes" } else { "rule_buy_no" };
            to_submit.push((i, req));
        }
        for (i, req) in to_submit {
            self.traded.insert((i, ticker.to_string()));
            ctx.submit(req);
            self.orders += 1;
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
                self.markets.insert(m.ticker.clone(), (m.series.clone(), m.title.clone(), m.close_ts_ms));
            }
            MarketEvent::BookSnapshot { ticker, .. } | MarketEvent::BookDelta { ticker, .. } | MarketEvent::BookLevel { ticker, .. } | MarketEvent::Ticker { ticker, .. } => {
                if self.markets.contains_key(ticker) {
                    let t = ticker.clone();
                    self.evaluate(&t, ctx);
                }
            }
            MarketEvent::Settlement { ticker, .. } => {
                self.markets.remove(ticker);
            }
            _ => {}
        }
    }
    fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({
            "kind": "rule_trader", "mode": "taker", "rules": self.cfg.rules.len(), "stake": self.cfg.stake, "orders": self.orders,
            "series": self.cfg.rules.iter().map(|r| r.scope.clone()).collect::<HashSet<_>>().into_iter().collect::<Vec<_>>(),
            "markets": self.markets.iter().map(|(t, (s, _, c))| serde_json::json!({"ticker": t, "series": s, "close_ts_ms": c})).collect::<Vec<_>>(),
            "rule_labels": self.cfg.rules.iter().map(|r| r.label.clone()).take(50).collect::<Vec<_>>(),
        })
    }
}
