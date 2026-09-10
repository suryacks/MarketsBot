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

use crate::btc15m::kalshi_tick;

/// How far a reported temperature can sit from the real one.
///
/// Observations come from IEM in whole degrees Fahrenheit — the unit Kalshi settles in — so a
/// reading of 89 means the truth lies in [88.5, 89.5). Half a degree is the whole error.
///
/// It used to be api.weather.gov, which publishes whole degrees CELSIUS: 32 C converts to
/// 89.6 F while the truth is anywhere in [88.7, 90.5), and that read Atlanta's real 89 F
/// maximum as 89.6, sold an 88-89 bucket as already lost, and watched it settle at 89. The
/// strategy had been validated against IEM all along, so the study and the live bot were
/// never the same measurement. They are now.
///
/// The remaining error is sampling: observations are hourly, so the true peak between them
/// can be missed. That biases the observed maximum DOWN and the minimum UP, which is the
/// safe direction — it costs trades, never correctness.
const OBSERVATION_TOLERANCE_F: f64 = 0.5;
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
    /// Trade rain markets on the radar nowcast (rain arriving before the gauge records it).
    pub use_nowcast: bool,
    /// mm of precipitation forecast in the next 60 minutes to treat rain as very likely.
    pub nowcast_mm: f64,
    /// Only buy rain YES on the nowcast below this price (above it there is no room).
    pub nowcast_max_px: f64,
    /// Trade temperature buckets the HRRR model says are already out of reach.
    pub use_hrrr: bool,
    /// °F the HRRR remaining-max must clear a bucket by before selling it.
    pub hrrr_margin_f: f64,
}

impl Default for WeatherLockConfig {
    fn default() -> Self {
        // Station and local-day offset for every temperature series, chosen by matching
        // Kalshi's own settled `expiration_value` against each candidate station's
        // observations (research/station_map.py, 1,913 market-days). KXHIGHNY is absent on
        // purpose: Central Park read 3F hotter than the official max once in 45 days, which
        // no workable margin absorbs. Across the 47 kept series a 1F margin leaves zero
        // days on which an observation-decided lock would have been wrong.
        let pairs = [
            ("KXHIGHAUS", "KAUS", -5), ("KXHIGHCHI", "KMDW", -5), ("KXHIGHDEN", "KDEN", -6),
            ("KXHIGHLAX", "KLAX", -7), ("KXHIGHMIA", "KMIA", -4), ("KXHIGHPHIL", "KPHL", -4),
            ("KXHIGHTATL", "KATL", -4), ("KXHIGHTBOS", "KBOS", -4), ("KXHIGHTDAL", "KDFW", -5),
            ("KXHIGHTDC", "KDCA", -4), ("KXHIGHTEWR", "KEWR", -4), ("KXHIGHTHOU", "KHOU", -5),
            ("KXHIGHTLV", "KLAS", -7), ("KXHIGHTMIN", "KMSP", -5), ("KXHIGHTNOLA", "KMSY", -5),
            ("KXHIGHTOKC", "KOKC", -5), ("KXHIGHTPHX", "KPHX", -7), ("KXHIGHTSAN", "KSAN", -7),
            ("KXHIGHTSATX", "KSAT", -5), ("KXHIGHTSDF", "KSDF", -4), ("KXHIGHTSEA", "KSEA", -7),
            ("KXHIGHTSFO", "KSFO", -7), ("KXHIGHTTTN", "KTTN", -4), ("KXLOWTATL", "KATL", -4),
            ("KXLOWTAUS", "KAUS", -5), ("KXLOWTBOS", "KBOS", -4), ("KXLOWTCHI", "KMDW", -5),
            ("KXLOWTDAL", "KDFW", -5), ("KXLOWTDC", "KDCA", -4), ("KXLOWTDEN", "KDEN", -6),
            ("KXLOWTEWR", "KEWR", -4), ("KXLOWTHOU", "KHOU", -5), ("KXLOWTLAX", "KLAX", -7),
            ("KXLOWTLV", "KLAS", -7), ("KXLOWTMIA", "KMIA", -4), ("KXLOWTMIN", "KMSP", -5),
            ("KXLOWTNOLA", "KMSY", -5), ("KXLOWTNYC", "KNYC", -4), ("KXLOWTOKC", "KOKC", -5),
            ("KXLOWTPHIL", "KPHL", -4), ("KXLOWTPHX", "KPHX", -7), ("KXLOWTSAN", "KSAN", -7),
            ("KXLOWTSATX", "KSAT", -5), ("KXLOWTSDF", "KSDF", -4), ("KXLOWTSEA", "KSEA", -7),
            ("KXLOWTSFO", "KSFO", -7), ("KXLOWTTTN", "KTTN", -4),
        ];
        Self {
            stations: pairs.iter().map(|(s, st, _)| (s.to_string(), st.to_string())).collect(),
            utc_offset_hours: pairs.iter().map(|(s, _, o)| (s.to_string(), *o)).collect(),
            min_edge: 0.02,
            stake: 2.0,
            margin_f: 0.0,
            use_nowcast: true,
            nowcast_mm: 1.0,
            nowcast_max_px: 0.75,
            use_hrrr: true,
            hrrr_margin_f: 4.0,
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
    /// Last touch the strategy saw. Kept so the dashboard can say why a decided market
    /// went untraded: the snapshot only publishes books we hold or have orders in, so
    /// without this a dead bucket with no bid is indistinguishable from one we cannot see.
    last_bid: Option<f64>,
    last_ask: Option<f64>,
    /// (lo, hi): YES iff lo ≤ max ≤ hi (hi = 999 for "greater" strikes)
    lo: f64,
    hi: f64,
}

pub struct WeatherLock {
    cfg: WeatherLockConfig,
    markets: HashMap<String, Mkt>,
    /// station -> (local day key, running max)
    run_max: HashMap<String, (i64, f64)>,
    /// station -> (local day key, running min). The mirror of `run_max`: a day's
    /// observed minimum is an upper bound on the official minimum, so it decides
    /// low-temperature markets the way the observed maximum decides highs.
    run_min: HashMap<String, (i64, f64)>,
    /// station -> (local day key, mm of measurable precipitation observed today)
    rain: HashMap<String, (i64, f64)>,
    /// KXRAIN ticker -> (station, close_ts_ms)
    rain_markets: HashMap<String, (String, i64)>,
    /// station -> (ts, mm expected in the next 60 min)
    nowcast: HashMap<String, (i64, f64)>,
    /// (station, local day) -> (ts, HRRR max °F for that day)
    hrrr: HashMap<(String, i64), (i64, f64)>,
    traded: HashSet<String>,
    pub orders: u64,
    pub decided: u64,
    pub nowcast_trades: u64,
    pub hrrr_trades: u64,
}

fn rain_station_offset(station: &str) -> i32 {
    ["ATL", "AUS", "BOS", "CHI", "DAL", "DC", "DEN", "EWR", "HOU", "LAX", "LV", "MIA", "MIN", "NOLA", "NYC", "OKC", "PHIL", "PHX", "SATX", "SEA", "SFO", "TTN"]
        .iter()
        .filter_map(|c| rain_station(c))
        .find(|(s, _)| *s == station)
        .map(|(_, o)| o)
        .unwrap_or(-5)
}

/// KXRAIN city → (NWS station, UTC offset of the local day)
fn rain_station(city: &str) -> Option<(&'static str, i32)> {
    Some(match city {
        "ATL" => ("KATL", -4), "AUS" => ("KAUS", -5), "BOS" => ("KBOS", -4), "CHI" => ("KORD", -5), "DAL" => ("KDFW", -5), "DC" => ("KDCA", -4),
        "DEN" => ("KDEN", -6), "EWR" => ("KEWR", -4), "HOU" => ("KIAH", -5), "LAX" => ("KLAX", -7), "LV" => ("KLAS", -7), "MIA" => ("KMIA", -4),
        "MIN" => ("KMSP", -5), "NOLA" => ("KMSY", -5), "NYC" => ("KNYC", -4), "OKC" => ("KOKC", -5), "PHIL" => ("KPHL", -4), "PHX" => ("KPHX", -7),
        "SATX" => ("KSAT", -5), "SEA" => ("KSEA", -7), "SFO" => ("KSFO", -7), "TTN" => ("KTTN", -4),
        _ => return None,
    })
}

impl WeatherLock {
    pub fn new(cfg: WeatherLockConfig) -> Self {
        Self {
            cfg,
            markets: HashMap::new(),
            run_max: HashMap::new(),
            run_min: HashMap::new(),
            rain: HashMap::new(),
            rain_markets: HashMap::new(),
            nowcast: HashMap::new(),
            hrrr: HashMap::new(),
            traded: HashSet::new(),
            orders: 0,
            decided: 0,
            nowcast_trades: 0,
            hrrr_trades: 0,
        }
    }

    /// Radar nowcast: rain is arriving at this station within the hour and the market
    /// has not repriced yet. Probabilistic — sized like any other directional trade.
    fn evaluate_nowcast(&mut self, station: &str, ctx: &mut dyn Context) {
        if !self.cfg.use_nowcast {
            return;
        }
        let Some(&(ts, mm)) = self.nowcast.get(station) else { return };
        let now = ctx.now_ms();
        if mm < self.cfg.nowcast_mm || now - ts > 20 * 60_000 {
            return;
        }
        let tickers: Vec<String> = self
            .rain_markets
            .iter()
            .filter(|(t, (st, close))| st == station && !self.traded.contains(*t) && *close > now + 30 * 60_000)
            .map(|(t, _)| t.clone())
            .collect();
        for t in tickers {
            let Some(book) = ctx.book(&t) else { continue };
            let Some((ask, aq)) = book.best_ask() else { continue };
            if ask.to_f64() > self.cfg.nowcast_max_px {
                continue;
            }
            let qty = (self.cfg.stake / ask.to_f64().max(0.02)).floor().min(aq.to_f64());
            if qty < 1.0 {
                continue;
            }
            tracing::info!(ticker = %t, station, nowcast_mm = mm, ask = %ask, "RAIN NOWCAST trade");
            // `traded` dies with the process; the exchange position is what survives a restart.
            if !ctx.position(&t).yes_qty.is_zero() {
                self.traded.insert(t);
                continue;
            }
            ctx.submit(OrderRequest::buy_yes(&t, ask, Fp::from_int(qty as i64), Tif::Ioc).tagged("rain_nowcast"));
            self.traded.insert(t);
            self.orders += 1;
            self.nowcast_trades += 1;
        }
    }

    /// HRRR: the 3 km model's remaining maximum for today is far above a bucket, so that
    /// bucket cannot be the day's max — sell it before the human forecast catches up.
    fn evaluate_hrrr(&mut self, station: &str, day: i64, ctx: &mut dyn Context) {
        if !self.cfg.use_hrrr {
            return;
        }
        let Some(&(ts, hmax)) = self.hrrr.get(&(station.to_string(), day)) else { return };
        let now = ctx.now_ms();
        if now - ts > 90 * 60_000 {
            return;
        }
        // Only markets whose own local day matches the forecast day.
        let tickers: Vec<String> = self
            .markets
            .iter()
            .filter(|(t, m)| {
                self.cfg.stations.get(&m.series).map(|s| s == station).unwrap_or(false)
                    // The forecast is the day's MAXIMUM. It says nothing about the day's
                    // minimum, so it must never be used to rule out a low-temperature bucket.
                    && !Self::is_low(&m.series)
                    && !self.traded.contains(*t)
                    && m.close_ts_ms > now + 30 * 60_000
                    && m.hi < 999.0
                    && hmax > m.hi + self.cfg.hrrr_margin_f
                    && self.day_key(&m.series, m.close_ts_ms) == day
            })
            .map(|(t, _)| t.clone())
            .collect();
        for t in tickers {
            let Some(book) = ctx.book(&t) else { continue };
            let Some((bid, bq)) = book.best_bid() else { continue };
            let edge = bid.to_f64() - ctx.fee_model(&t).fee_per_contract(bid, false);
            if edge < self.cfg.min_edge {
                continue;
            }
            let no_px = 1.0 - bid.to_f64();
            let qty = (self.cfg.stake / no_px.max(0.02)).floor().min(bq.to_f64());
            if qty < 1.0 {
                continue;
            }
            tracing::info!(ticker = %t, station, hrrr_max = hmax, bid = %bid, "HRRR trade (bucket out of reach)");
            // `traded` dies with the process; the exchange position is what survives a restart.
            if !ctx.position(&t).yes_qty.is_zero() {
                self.traded.insert(t);
                continue;
            }
            ctx.submit(OrderRequest::sell_yes(&t, bid, Fp::from_int(qty as i64), Tif::Ioc).tagged("hrrr_no"));
            self.traded.insert(t);
            self.orders += 1;
            self.hrrr_trades += 1;
        }
    }

    /// Rain markets decided YES by measurable precipitation today at their station.
    fn evaluate_rain(&mut self, station: &str, ctx: &mut dyn Context) {
        let Some(&(day, mm)) = self.rain.get(station) else { return };
        if mm < 0.25 {
            return; // < 0.01" — not measurable
        }
        let now = ctx.now_ms();
        let tickers: Vec<String> = self
            .rain_markets
            .iter()
            .filter(|(t, (st, close))| st == station && !self.traded.contains(*t) && *close > now)
            .map(|(t, _)| t.clone())
            .collect();
        for t in tickers {
            let (_, close) = self.rain_markets[&t];
            let city = t.rsplit('-').next().unwrap_or("");
            let off = rain_station(city).map(|(_, o)| o).unwrap_or(0) as i64;
            let market_day = (close / 1000 + off * 3600 - 6 * 3600).div_euclid(86_400);
            if market_day != day {
                continue;
            }
            self.decided += 1;
            let Some(book) = ctx.book(&t) else { continue };
            let Some((ask, aq)) = book.best_ask() else { continue };
            let edge = 1.0 - ask.to_f64() - ctx.fee_model(&t).fee_per_contract(ask, false);
            if edge < self.cfg.min_edge {
                continue;
            }
            let qty = (self.cfg.stake / ask.to_f64().max(0.02)).floor().min(aq.to_f64());
            if qty < 1.0 {
                continue;
            }
            tracing::info!(ticker = %t, station, precip_mm = mm, ask = %ask, "RAIN LOCK trade");
            // `traded` dies with the process; the exchange position is what survives a restart.
            if !ctx.position(&t).yes_qty.is_zero() {
                self.traded.insert(t);
                continue;
            }
            ctx.submit(OrderRequest::buy_yes(&t, ask, Fp::from_int(qty as i64), Tif::Ioc).tagged("rain_lock_yes"));
            self.traded.insert(t);
            self.orders += 1;
        }
    }

    /// `(lo, hi)`: the market pays iff the official value lands in `[lo, hi]`.
    /// `hi = 999` means no upper bound, `lo = -999` no lower bound.
    fn parse_market(m: &mb_core::MarketInfo) -> Option<(f64, f64)> {
        let tick = m.ticker.rsplit('-').next()?;
        if let Some(rest) = tick.strip_prefix('B') {
            let c: f64 = rest.parse().ok()?;
            return Some((c.floor(), c.ceil()));
        }
        match m.strike_type.as_str() {
            "greater" => m.floor_strike.map(|k| (k.floor() + 1.0, 999.0)),
            "greater_or_equal" => m.floor_strike.map(|k| (k.ceil(), 999.0)),
            "less" => m.cap_strike.map(|k| (-999.0, k.ceil() - 1.0)),
            "less_or_equal" => m.cap_strike.map(|k| (-999.0, k.floor())),
            _ => None,
        }
    }

    /// Does this series settle on the day's minimum rather than its maximum?
    fn is_low(series: &str) -> bool {
        series.starts_with("KXLOW")
    }

    /// Local day key for a market: the local date of (close − 6 h).
    fn day_key(&self, series: &str, ts_ms: i64) -> i64 {
        let off = self.cfg.utc_offset_hours.get(series).copied().unwrap_or(0) as i64;
        (ts_ms / 1000 + off * 3600 - 6 * 3600).div_euclid(86_400)
    }

    fn evaluate_all(&mut self, station: &str, ctx: &mut dyn Context) {
        let hi_obs = self.run_max.get(station).copied();
        let lo_obs = self.run_min.get(station).copied();
        let Some(day) = hi_obs.map(|(d, _)| d).or_else(|| lo_obs.map(|(d, _)| d)) else { return };
        let now = ctx.now_ms();
        let tickers: Vec<String> = self
            .markets
            .iter()
            .filter(|(t, m)| self.cfg.stations.get(&m.series).map(|s| s == station).unwrap_or(false) && !self.traded.contains(*t) && m.close_ts_ms > now && self.day_key(&m.series, m.close_ts_ms) == day)
            .map(|(t, _)| t.clone())
            .collect();
        for t in tickers {
            if let Some(b) = ctx.book(&t) {
                let (bb, ba) = (b.best_bid().map(|(p, _)| p.to_f64()), b.best_ask().map(|(p, _)| p.to_f64()));
                if let Some(mm) = self.markets.get_mut(&t) {
                    mm.last_bid = bb;
                    mm.last_ask = ba;
                }
            }
            let m = self.markets[&t].clone();
            // Observations only ever move one way within a day: the running maximum can
            // rise and the running minimum can fall. So each is a one-sided bound on the
            // official number, and only the side it has already passed is decided.
            // Compare in the units the market settles in. Kalshi resolves temperature to whole
            // degrees, while the observation is tenths, so an unrounded compare calls a bucket
            // dead over a fraction the settlement will round away: a Dallas 101-102 bucket read
            // 102.2 and looked lost, but the official max rounds to 102 and the bucket wins --
            // which is why the market was still bidding 99c on it.
            // Settlement is a whole number of degrees F, so a bucket is only dead once the
            // official value must be at least one degree past it. Compare against the
            // conservative end of the observation's range, never its midpoint.
            let (decided_yes, decided_no, obs) = if Self::is_low(&m.series) {
                match lo_obs {
                    // The minimum only falls, so it bounds the official value from above —
                    // and the warmest it could really be is the reading plus the quantization.
                    Some((d, mn)) if d == day => {
                        let warmest = mn + OBSERVATION_TOLERANCE_F + self.cfg.margin_f;
                        (warmest <= m.hi && m.lo <= -999.0, warmest <= m.lo - 1.0, mn)
                    }
                    _ => continue,
                }
            } else {
                match hi_obs {
                    // The maximum only rises, so it bounds the official value from below —
                    // and the coolest it could really be is the reading minus the quantization.
                    Some((d, mx)) if d == day => {
                        let coolest = mx - OBSERVATION_TOLERANCE_F - self.cfg.margin_f;
                        (coolest >= m.lo && m.hi >= 999.0, coolest >= m.hi + 1.0 && m.hi < 999.0, mx)
                    }
                    _ => continue,
                }
            };
            if !decided_yes && !decided_no {
                continue;
            }
            self.decided += 1;
            let Some(book) = ctx.book(&t) else { continue };
            let fee = ctx.fee_model(&t);
            // Price the IOC at the WORST level still worth taking, not at the touch. An order
            // priced exactly at the best quote fills nothing if that quote is pulled in the
            // milliseconds before it lands -- which is how the first San Francisco lock was
            // missed -- whereas a limit set at our own floor sweeps every level in between and
            // can still never fill worse than `min_edge`.
            let req = if decided_yes {
                let Some((ask, _)) = book.best_ask() else { continue };
                let edge = 1.0 - ask.to_f64() - fee.fee_per_contract(ask, false);
                if edge < self.cfg.min_edge {
                    continue;
                }
                let floor_px = Fp::from_f64(1.0 - self.cfg.min_edge - fee.fee_per_contract(ask, false));
                let limit = floor_px.round_down_to(kalshi_tick(floor_px)).max(ask);
                let qty = (self.cfg.stake / limit.to_f64().max(0.02)).floor();
                if qty < 1.0 {
                    continue;
                }
                OrderRequest::buy_yes(&t, limit, Fp::from_int(qty as i64), Tif::Ioc).tagged("wx_lock_yes")
            } else {
                let Some((bid, _)) = book.best_bid() else { continue };
                let edge = bid.to_f64() - fee.fee_per_contract(bid, false);
                if edge < self.cfg.min_edge {
                    continue;
                }
                let floor_px = Fp::from_f64(self.cfg.min_edge + fee.fee_per_contract(bid, false));
                let limit = floor_px.round_up_to(kalshi_tick(floor_px)).min(bid);
                let qty = (self.cfg.stake / (1.0 - limit.to_f64()).max(0.02)).floor();
                if qty < 1.0 {
                    continue;
                }
                OrderRequest::sell_yes(&t, limit, Fp::from_int(qty as i64), Tif::Ioc).tagged("wx_lock_no")
            };
            tracing::info!(ticker = %t, station, observed = obs, kind = if Self::is_low(&m.series) { "min" } else { "max" }, lo = m.lo, hi = m.hi, "WEATHER LOCK trade");
            // `traded` dies with the process; the exchange position is what survives a restart.
            if !ctx.position(&t).yes_qty.is_zero() {
                self.traded.insert(t);
                continue;
            }
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
                if m.series == "KXRAIN" {
                    let city = m.ticker.rsplit('-').next().unwrap_or("");
                    if let Some((st, _)) = rain_station(city) {
                        self.rain_markets.insert(m.ticker.clone(), (st.to_string(), m.close_ts_ms));
                    }
                } else if self.cfg.stations.contains_key(&m.series)
                    && let Some((lo, hi)) = Self::parse_market(m)
                {
                    self.markets.insert(m.ticker.clone(), Mkt { series: m.series.clone(), close_ts_ms: m.close_ts_ms, lo, hi, last_bid: None, last_ask: None });
                }
            }
            MarketEvent::Ref(r) if r.source == "nowcast" => {
                if let Some(st) = r.symbol.strip_suffix(":nowcast_precip_60m") {
                    let st = st.to_string();
                    self.nowcast.insert(st.clone(), (r.ts_ms, r.px));
                    self.evaluate_nowcast(&st, ctx);
                } else if let Some(rest) = r.symbol.split(":hrrr_max_f:").nth(1) {
                    let st = r.symbol.split(':').next().unwrap_or("").to_string();
                    if let Ok(day) = rest.parse::<i64>() {
                        self.hrrr.insert((st.clone(), day), (r.ts_ms, r.px));
                        self.evaluate_hrrr(&st, day, ctx);
                    }
                }
            }
            MarketEvent::Ref(r) if r.source == "nws" && r.symbol.ends_with(":precip_mm") => {
                let station = r.symbol.trim_end_matches(":precip_mm").to_string();
                let off = rain_station_offset(&station);
                let day = (r.ts_ms / 1000 + off as i64 * 3600).div_euclid(86_400);
                let e = self.rain.entry(station.clone()).or_insert((day, 0.0));
                if e.0 != day {
                    *e = (day, 0.0);
                }
                e.1 += r.px; // accumulate last-hour mm (obs are ~hourly; specials may double count slightly — conservative direction is fine)
                self.evaluate_rain(&station, ctx);
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
                let e = self.run_min.entry(r.symbol.clone()).or_insert((day, r.px));
                if e.0 != day {
                    *e = (day, r.px);
                } else if r.px < e.1 {
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
                self.rain_markets.remove(ticker);
            }
            _ => {}
        }
    }
    fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"kind": "weather_lock", "mode": "taker", "orders": self.orders, "decided_moments": self.decided,
                           "nowcast_trades": self.nowcast_trades, "hrrr_trades": self.hrrr_trades,
                           "nowcast": self.nowcast.iter().filter(|(_, (_, mm))| *mm > 0.0).map(|(s, (_, mm))| serde_json::json!({"station": s, "precip_60m_mm": mm})).collect::<Vec<_>>(),
                           "hrrr_max": self.hrrr.iter().map(|((s, d), (_, f))| serde_json::json!({"station": s, "day": d, "max_f": f})).collect::<Vec<_>>(),
                           "series": self.cfg.stations.keys().cloned().chain(std::iter::once("KXRAIN".to_string())).collect::<Vec<_>>(),
                           "rain_markets": self.rain_markets.len(),
                           "rain_today_mm": self.rain.iter().filter(|(_, (_, mm))| *mm > 0.0).map(|(s, (_, mm))| serde_json::json!({"station": s, "mm": mm})).collect::<Vec<_>>(),
                           "running_max": self.run_max.iter().map(|(s, (d, m))| serde_json::json!({"station": s, "day": d, "max_f": m, "min_f": self.run_min.get(s).map(|(_, v)| *v)})).collect::<Vec<_>>(),
                           "markets": self.markets.iter().map(|(t, m)| serde_json::json!({"ticker": t, "series": m.series, "close_ts_ms": m.close_ts_ms, "lo": m.lo, "hi": m.hi})).collect::<Vec<_>>(),
                           "tolerance_f": OBSERVATION_TOLERANCE_F,
                           // The reasoning behind each market, so the dashboard can show the
                           // arithmetic rather than only the conclusion.
                           "thinking": self.markets.iter().filter_map(|(t, m)| {
                               let station = self.cfg.stations.get(&m.series)?;
                               let low = Self::is_low(&m.series);
                               let (day, obs) = if low { *self.run_min.get(station)? } else { *self.run_max.get(station)? };
                               if self.day_key(&m.series, m.close_ts_ms) != day {
                                   return None; // a different day's market
                               }
                               let (bound, needs, dead) = if low {
                                   let warmest = obs + OBSERVATION_TOLERANCE_F + self.cfg.margin_f;
                                   (warmest, m.lo - 1.0, warmest <= m.lo - 1.0)
                               } else {
                                   let coolest = obs - OBSERVATION_TOLERANCE_F - self.cfg.margin_f;
                                   (coolest, m.hi + 1.0, coolest >= m.hi + 1.0 && m.hi < 999.0)
                               };
                               let bucket = if m.hi >= 999.0 { format!("{}F or above", m.lo) }
                                            else if m.lo <= -999.0 { format!("below {}F", m.hi + 1.0) }
                                            else { format!("{}-{}F", m.lo, m.hi) };
                               let explain = if low {
                                   format!("{station} low so far {obs:.0}F; warmest it could really be {bound:.1}F; needs {needs:.0}F or colder to kill {bucket}")
                               } else {
                                   format!("{station} high so far {obs:.0}F; coolest it could really be {bound:.1}F; needs {needs:.0}F or hotter to kill {bucket}")
                               };
                               Some(serde_json::json!({"ticker": t, "station": station, "kind": if low {"min"} else {"max"},
                                   "bucket": bucket, "observed": obs, "conservative": bound, "needs": needs,
                                   "verdict": if dead { "DEAD - can be sold" } else { "still possible" },
                                   "bid": m.last_bid, "ask": m.last_ask,
                                   "traded": self.traded.contains(t), "explain": explain}))
                           }).collect::<Vec<_>>()})
    }
}
