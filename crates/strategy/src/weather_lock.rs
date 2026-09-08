//! Weather settlement lock: the daily-high market settles on the day's maximum,
//! which can only rise. Once the observed running max has passed a "> X°"
//! strike the market is decided YES; once it has passed above a "between"
//! bucket, that bucket is decided NO. Buy the decided side wherever the book
//! still offers more than fees.
//!
//! Observations arrive as `Ref` events (source "nws", symbol = station id, °F).
//! Kalshi's official max comes from the NWS climate report, which is ≥ any
//! hourly/special observation, so "decided by observation" is one-directional
//! and safe; the residual risk is a station/report mismatch.

use anyhow::{Context as _, Result};
use mb_core::{Context, Fp, MarketEvent, OrderRequest, Strategy, Tif};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WeatherLockConfig {
    /// series -> station id
    pub stations: HashMap<String, String>,
    /// series -> UTC offset hours of the local day (negative for the US)
    pub utc_offset_hours: HashMap<String, i32>,
    /// Minimum edge after fee before taking (1 − ask for YES, bid for NO).
    pub min_edge: f64,
    pub stake: f64,
    /// Safety margin in °F above the strike before calling it decided (station/report mismatch).
    pub margin_f: f64,
}

impl Default for WeatherLockConfig {
    fn default() -> Self {
        let pairs = [("KXHIGHNY", "KNYC", -4), ("KXHIGHCHI", "KMDW", -5), ("KXHIGHMIA", "KMIA", -4), ("KXHIGHLAX", "KLAX", -7), ("KXHIGHAUS", "KAUS", -5), ("KXHIGHPHIL", "KPHL", -4), ("KXHIGHDEN", "KDEN", -6)];
        Self {
            stations: pairs.iter().map(|(s, st, _)| (s.to_string(), st.to_string())).collect(),
            utc_offset_hours: pairs.iter().map(|(s, _, o)| (s.to_string(), *o)).collect(),
            min_edge: 0.02,
            stake: 2.0,
            margin_f: 0.0,
        }
    }
}

impl WeatherLockConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let s = std::fs::read_to_string(path.as_ref()).with_context(|| format!("reading {}", path.as_ref().display()))?;
        toml::from_str(&s).context("parsing weather lock config")
    }
}

#[derive(Clone, Debug)]
struct Mkt {
    series: String,
    close_ts_ms: i64,
    /// (lo, hi): YES iff lo ≤ max ≤ hi (hi = 999 for "greater" strikes)
    lo: f64,
    hi: f64,
}

pub struct WeatherLock {
    cfg: WeatherLockConfig,
    markets: HashMap<String, Mkt>,
    /// station -> (local day key, running max)
    run_max: HashMap<String, (i64, f64)>,
    traded: HashSet<String>,
    pub orders: u64,
    pub decided: u64,
}

impl WeatherLock {
    pub fn new(cfg: WeatherLockConfig) -> Self {
        Self {
            cfg,
            markets: HashMap::new(),
            run_max: HashMap::new(),
            traded: HashSet::new(),
            orders: 0,
            decided: 0,
        }
    }

    fn parse_market(m: &mb_core::MarketInfo) -> Option<(f64, f64)> {
        let tick = m.ticker.rsplit('-').next()?;
        if let Some(rest) = tick.strip_prefix('B') {
            let c: f64 = rest.parse().ok()?;
            return Some((c.floor(), c.ceil()));
        }
        match m.strike_type.as_str() {
            "greater" => m.floor_strike.map(|k| (k.floor() + 1.0, 999.0)),
            "greater_or_equal" => m.floor_strike.map(|k| (k.ceil(), 999.0)),
            _ => None,
        }
    }

    /// Local day key for a market: the local date of (close − 6 h).
    fn day_key(&self, series: &str, ts_ms: i64) -> i64 {
        let off = self.cfg.utc_offset_hours.get(series).copied().unwrap_or(0) as i64;
        (ts_ms / 1000 + off * 3600 - 6 * 3600).div_euclid(86_400)
    }

    fn evaluate_all(&mut self, station: &str, ctx: &mut dyn Context) {
        let Some(&(day, mx)) = self.run_max.get(station) else { return };
        let now = ctx.now_ms();
        let tickers: Vec<String> = self
            .markets
            .iter()
            .filter(|(t, m)| self.cfg.stations.get(&m.series).map(|s| s == station).unwrap_or(false) && !self.traded.contains(*t) && m.close_ts_ms > now && self.day_key(&m.series, m.close_ts_ms) == day)
            .map(|(t, _)| t.clone())
            .collect();
        for t in tickers {
            let m = self.markets[&t].clone();
            let decided_yes = mx >= m.lo + self.cfg.margin_f && m.hi >= 999.0; // "greater" strike passed
            let decided_no = mx > m.hi + self.cfg.margin_f && m.hi < 999.0; // bucket overshot
            if !decided_yes && !decided_no {
                continue;
            }
            self.decided += 1;
            let Some(book) = ctx.book(&t) else { continue };
            let fee = ctx.fee_model(&t);
            let req = if decided_yes {
                let Some((ask, aq)) = book.best_ask() else { continue };
                let edge = 1.0 - ask.to_f64() - fee.fee_per_contract(ask, false);
                if edge < self.cfg.min_edge {
                    continue;
                }
                let qty = (self.cfg.stake / ask.to_f64().max(0.02)).floor().min(aq.to_f64());
                if qty < 1.0 {
                    continue;
                }
                OrderRequest::buy_yes(&t, ask, Fp::from_int(qty as i64), Tif::Ioc).tagged("wx_lock_yes")
            } else {
                let Some((bid, bq)) = book.best_bid() else { continue };
                let edge = bid.to_f64() - fee.fee_per_contract(bid, false);
                if edge < self.cfg.min_edge {
                    continue;
                }
                let no_px = 1.0 - bid.to_f64();
                let qty = (self.cfg.stake / no_px.max(0.02)).floor().min(bq.to_f64());
                if qty < 1.0 {
                    continue;
                }
                OrderRequest::sell_yes(&t, bid, Fp::from_int(qty as i64), Tif::Ioc).tagged("wx_lock_no")
            };
            tracing::info!(ticker = %t, station, running_max = mx, lo = m.lo, hi = m.hi, "WEATHER LOCK trade");
            ctx.submit(req);
            self.traded.insert(t);
            self.orders += 1;
        }
    }
}

impl Strategy for WeatherLock {
    fn name(&self) -> &str {
        "weather_lock"
    }
    fn on_event(&mut self, ev: &MarketEvent, ctx: &mut dyn Context) {
        match ev {
            MarketEvent::Market(m) => {
                if self.cfg.stations.contains_key(&m.series)
                    && let Some((lo, hi)) = Self::parse_market(m)
                {
                    self.markets.insert(m.ticker.clone(), Mkt { series: m.series.clone(), close_ts_ms: m.close_ts_ms, lo, hi });
                }
            }
            MarketEvent::Ref(r) if r.source == "nws" => {
                // which series does this station serve? (day key needs the series' offset)
                let series = self.cfg.stations.iter().find(|(_, st)| **st == r.symbol).map(|(s, _)| s.clone());
                let Some(series) = series else { return };
                let day = self.day_key(&series, r.ts_ms + 6 * 3_600_000); // obs ts → local day (undo the −6 h in day_key)
                let e = self.run_max.entry(r.symbol.clone()).or_insert((day, r.px));
                if e.0 != day {
                    *e = (day, r.px);
                } else if r.px > e.1 {
                    e.1 = r.px;
                }
                let st = r.symbol.clone();
                self.evaluate_all(&st, ctx);
            }
            MarketEvent::BookSnapshot { ticker, .. } | MarketEvent::BookDelta { ticker, .. } | MarketEvent::BookLevel { ticker, .. } | MarketEvent::Ticker { ticker, .. } => {
                if let Some(m) = self.markets.get(ticker).cloned()
                    && let Some(st) = self.cfg.stations.get(&m.series).cloned()
                {
                    self.evaluate_all(&st, ctx);
                }
            }
            MarketEvent::Settlement { ticker, .. } => {
                self.markets.remove(ticker);
            }
            _ => {}
        }
    }
    fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"kind": "weather_lock", "mode": "taker", "orders": self.orders, "decided_moments": self.decided,
                           "series": self.cfg.stations.keys().cloned().collect::<Vec<_>>(),
                           "running_max": self.run_max.iter().map(|(s, (d, m))| serde_json::json!({"station": s, "day": d, "max_f": m})).collect::<Vec<_>>(),
                           "markets": self.markets.iter().map(|(t, m)| serde_json::json!({"ticker": t, "series": m.series, "close_ts_ms": m.close_ts_ms, "lo": m.lo, "hi": m.hi})).collect::<Vec<_>>()})
    }
}
