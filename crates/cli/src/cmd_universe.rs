//! Strategy universe: thousands of parametrized strategies evaluated in memory
//! on the wide dataset, with walk-forward validation.
//!
//! One *sample* = one market observed at one horizon before close, with the
//! price at that moment and simple path features. A *strategy* = (family,
//! scope, horizon, predicate over the sample, side). Trades are taken at the
//! ask (buy YES) or bid (buy NO) with Kalshi's taker fee and held to
//! settlement, so every strategy is scored on identical, fee-inclusive terms.
//!
//! Walk-forward: markets are split by close time at the median. A strategy is
//! selected on the first half (IS) and judged on the second (OOS). With
//! thousands of candidates, ~2.5 % pass IS by luck; only OOS counts.

use anyhow::Result;
use clap::Args as ClapArgs;
use mb_core::{FeeModel, Fp};
use mb_data::{read_dir, DsMarket, DsPrice};
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::info;

#[derive(ClapArgs, Debug)]
pub struct Args {
    #[arg(long, default_value = "data/dataset")]
    pub data: PathBuf,
    #[arg(long, default_value = "reports")]
    pub out: PathBuf,
    /// Dollars per trade (position sizing for the $-PnL columns)
    #[arg(long, default_value_t = 2.0)]
    pub stake: f64,
    /// Minimum in-sample trades to consider a strategy at all
    #[arg(long, default_value_t = 30)]
    pub min_n: usize,
}

#[derive(Clone)]
struct Sample {
    series: u32,
    category: u32,
    horizon: i64,
    hour_utc: u8,
    weekend: bool,
    mid: f64,
    bid: f64,
    ask: f64,
    /// mid change over the previous k candles (k = 1, 3, 6, 12); NaN if unavailable
    ret: [f64; 4],
    /// mid relative to the path's max/min so far: +1 at a new high, −1 at a new low, else 0
    extreme: i8,
    /// mid change since the market's first observed candle
    ret_open: f64,
    yes: bool,
    close_ts: i64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize)]
enum Side {
    BuyYes,
    BuyNo,
}

#[derive(Clone, Serialize)]
struct StrategyDef {
    family: &'static str,
    scope: String,
    horizon: i64,
    params: String,
    side: Side,
}

#[derive(Serialize, Clone, Default)]
struct Stats {
    n: usize,
    ev: f64,
    t: f64,
    win: f64,
    pnl_stake: f64,
}

#[derive(Serialize)]
struct Row {
    #[serde(flatten)]
    def: StrategyDef,
    is: Stats,
    oos: Stats,
    verdict: &'static str,
}

fn stats(pnls: &[f64], stake: f64, prices: &[f64]) -> Stats {
    let n = pnls.len();
    if n == 0 {
        return Stats::default();
    }
    let m = pnls.iter().sum::<f64>() / n as f64;
    let var = if n > 1 { pnls.iter().map(|p| (p - m).powi(2)).sum::<f64>() / (n as f64 - 1.0) } else { 0.0 };
    let se = (var / n as f64).sqrt();
    // dollars: stake / price contracts per trade
    let pnl_stake: f64 = pnls.iter().zip(prices).map(|(p, px)| p * (stake / px.max(0.02))).sum();
    Stats {
        n,
        ev: m,
        t: if se > 0.0 { m / se } else { 0.0 },
        win: pnls.iter().filter(|p| **p > 0.0).count() as f64 / n as f64,
        pnl_stake,
    }
}

pub async fn run(a: Args) -> Result<()> {
    let markets: Vec<DsMarket> = read_dir(a.data.join("markets"))?;
    let prices: Vec<DsPrice> = read_dir(a.data.join("prices"))?;
    info!(markets = markets.len(), candles = prices.len(), "dataset loaded");
    if markets.is_empty() {
        anyhow::bail!("empty dataset — run `mbot build-dataset` first");
    }
    let mut series_ids: HashMap<String, u32> = HashMap::new();
    let mut cat_ids: HashMap<String, u32> = HashMap::new();
    let mut series_names = Vec::new();
    let mut cat_names = Vec::new();
    let mut by_ticker: HashMap<&str, Vec<&DsPrice>> = HashMap::new();
    for p in &prices {
        by_ticker.entry(p.ticker.as_str()).or_default().push(p);
    }
    for v in by_ticker.values_mut() {
        v.sort_by_key(|p| p.ts);
    }

    // ---- samples ----
    let short_h = [120i64, 300, 600, 900, 1800];
    let long_h = [3600i64, 3 * 3600, 6 * 3600, 12 * 3600, 24 * 3600, 48 * 3600];
    let mut samples: Vec<Sample> = Vec::new();
    for m in &markets {
        let Some(path) = by_ticker.get(m.ticker.as_str()) else { continue };
        if path.len() < 3 {
            continue;
        }
        let sid = *series_ids.entry(m.series.clone()).or_insert_with(|| {
            series_names.push(m.series.clone());
            (series_names.len() - 1) as u32
        });
        let cid = *cat_ids.entry(m.category.clone()).or_insert_with(|| {
            cat_names.push(m.category.clone());
            (cat_names.len() - 1) as u32
        });
        let mids: Vec<Option<f64>> = path
            .iter()
            .map(|p| match (p.bid, p.ask) {
                (Some(b), Some(a)) if a > b => Some((a + b) / 2.0),
                _ => p.last,
            })
            .collect();
        let first_mid = mids.iter().flatten().next().copied();
        let duration = m.close_ts - m.open_ts;
        let horizons: &[i64] = if duration <= 3 * 3600 { &short_h } else { &long_h };
        for &h in horizons {
            if h >= duration {
                continue;
            }
            // last candle with secs_to_close >= h
            let Some(i) = (0..path.len()).rev().find(|&i| path[i].secs_to_close >= h) else { continue };
            let p = path[i];
            let step = if duration <= 3 * 3600 { 60 } else { 3600 };
            if p.secs_to_close - h > 3 * step {
                continue; // stale
            }
            let Some(mid) = mids[i] else { continue };
            let (bid, ask) = match (p.bid, p.ask) {
                (Some(b), Some(a)) if a > b => (b, a),
                _ => ((mid - 0.01).max(0.01), (mid + 0.01).min(0.99)),
            };
            if !(0.02..=0.98).contains(&mid) {
                continue;
            }
            let mut ret = [f64::NAN; 4];
            for (j, k) in [1usize, 3, 6, 12].iter().enumerate() {
                if i >= *k
                    && let Some(prev) = mids[i - k]
                {
                    ret[j] = mid - prev;
                }
            }
            let (mut hi, mut lo) = (f64::MIN, f64::MAX);
            for x in mids[..i].iter().flatten() {
                hi = hi.max(*x);
                lo = lo.min(*x);
            }
            let extreme = if i == 0 { 0 } else if mid >= hi { 1 } else if mid <= lo { -1 } else { 0 };
            let dt = chrono::DateTime::from_timestamp(p.ts, 0).unwrap();
            use chrono::{Datelike, Timelike};
            samples.push(Sample {
                series: sid,
                category: cid,
                horizon: h,
                hour_utc: dt.hour() as u8,
                weekend: matches!(dt.weekday(), chrono::Weekday::Sat | chrono::Weekday::Sun),
                mid,
                bid,
                ask,
                ret,
                extreme,
                ret_open: first_mid.map(|f| mid - f).unwrap_or(f64::NAN),
                yes: m.result_yes,
                close_ts: m.close_ts,
            });
        }
    }
    info!(samples = samples.len(), series = series_names.len(), categories = cat_names.len(), "samples built");
    let mut closes: Vec<i64> = markets.iter().map(|m| m.close_ts).collect();
    closes.sort();
    let split_ts = closes[closes.len() / 2];

    // ---- strategy generation ----
    type Pred = Box<dyn Fn(&Sample) -> bool + Sync>;
    let mut strategies: Vec<(StrategyDef, Pred)> = Vec::new();
    let all_h: Vec<i64> = short_h.iter().chain(long_h.iter()).copied().collect();
    let scopes: Vec<(String, Box<dyn Fn(&Sample) -> bool + Sync>)> = {
        let mut v: Vec<(String, Box<dyn Fn(&Sample) -> bool + Sync>)> = vec![("ALL".into(), Box::new(|_| true))];
        for (i, c) in cat_names.iter().enumerate() {
            let id = i as u32;
            v.push((format!("CAT:{c}"), Box::new(move |s| s.category == id)));
        }
        for (i, sname) in series_names.iter().enumerate() {
            let id = i as u32;
            v.push((sname.clone(), Box::new(move |s| s.series == id)));
        }
        v
    };
    // Family 1: price-bucket rules (favorite-longshot / calibration), 5¢ buckets
    for (scope, _) in &scopes {
        for &h in &all_h {
            for b in 0..20 {
                let lo = b as f64 / 20.0;
                let hi = lo + 0.05;
                for side in [Side::BuyYes, Side::BuyNo] {
                    let sc = scope.clone();
                    strategies.push((
                        StrategyDef { family: "price-bucket", scope: sc, horizon: h, params: format!("px {lo:.2}-{hi:.2}"), side },
                        Box::new(move |s| s.mid >= lo && s.mid < hi),
                    ));
                }
            }
        }
    }
    // Family 2: Yes-bias — always buy NO / always buy YES in a scope (category & series level)
    for (scope, _) in &scopes {
        for &h in &all_h {
            for side in [Side::BuyYes, Side::BuyNo] {
                strategies.push((StrategyDef { family: "side-bias", scope: scope.clone(), horizon: h, params: "any price".into(), side }, Box::new(|_| true)));
            }
        }
    }
    // Family 3: momentum / reversal on the recent path (category & ALL scope)
    for (scope, _) in scopes.iter().filter(|(n, _)| n == "ALL" || n.starts_with("CAT:")) {
        for &h in &all_h {
            for (kj, k) in [1usize, 3, 6, 12].iter().enumerate() {
                for &x in &[0.03f64, 0.05, 0.10] {
                    for up in [true, false] {
                        for side in [Side::BuyYes, Side::BuyNo] {
                            let mode = match (up, side) {
                                (true, Side::BuyYes) | (false, Side::BuyNo) => "momentum",
                                _ => "reversal",
                            };
                            strategies.push((
                                StrategyDef { family: if mode == "momentum" { "momentum" } else { "reversal" }, scope: scope.clone(), horizon: h, params: format!("{} {:+.2} over {k} candles", if up { "up" } else { "down" }, if up { x } else { -x }), side },
                                Box::new(move |s| { let r = s.ret[kj]; r.is_finite() && if up { r >= x } else { r <= -x } }),
                            ));
                        }
                    }
                }
            }
            // new high / new low
            for (ext, label) in [(1i8, "new high"), (-1i8, "new low")] {
                for side in [Side::BuyYes, Side::BuyNo] {
                    strategies.push((StrategyDef { family: "breakout", scope: scope.clone(), horizon: h, params: label.into(), side }, Box::new(move |s| s.extreme == ext)));
                }
            }
            // drift since open (informed-flow proxy)
            for &x in &[0.10f64, 0.20, 0.30] {
                for up in [true, false] {
                    for side in [Side::BuyYes, Side::BuyNo] {
                        strategies.push((
                            StrategyDef { family: "drift-since-open", scope: scope.clone(), horizon: h, params: format!("{} {:+.2} since open", if up { "up" } else { "down" }, if up { x } else { -x }), side },
                            Box::new(move |s| s.ret_open.is_finite() && if up { s.ret_open >= x } else { s.ret_open <= -x }),
                        ));
                    }
                }
            }
        }
    }
    // Family 4: time-of-day / weekend filters on price buckets (category & ALL)
    for (scope, _) in scopes.iter().filter(|(n, _)| n == "ALL" || n.starts_with("CAT:")) {
        for &h in &all_h {
            for (tl, tf) in [("US night 04-12 UTC", 0u8), ("US day 12-20 UTC", 1), ("US evening 20-04 UTC", 2), ("weekend", 3)] {
                for b in [0usize, 1, 2, 3, 16, 17, 18, 19] {
                    let lo = b as f64 / 20.0;
                    let hi = lo + 0.05;
                    for side in [Side::BuyYes, Side::BuyNo] {
                        strategies.push((
                            StrategyDef { family: "time-filter", scope: scope.clone(), horizon: h, params: format!("{tl}, px {lo:.2}-{hi:.2}"), side },
                            Box::new(move |s| {
                                let t_ok = match tf { 0 => (4..12).contains(&s.hour_utc), 1 => (12..20).contains(&s.hour_utc), 2 => s.hour_utc >= 20 || s.hour_utc < 4, _ => s.weekend };
                                t_ok && s.mid >= lo && s.mid < hi
                            }),
                        ));
                    }
                }
            }
        }
    }
    info!(strategies = strategies.len(), "universe generated");

    // ---- evaluation ----
    let fee = FeeModel::kalshi_default();
    let scope_pred: HashMap<&str, &(String, Box<dyn Fn(&Sample) -> bool + Sync>)> = scopes.iter().map(|s| (s.0.as_str(), s)).collect();
    let mut rows: Vec<Row> = Vec::new();
    let t0 = std::time::Instant::now();
    for (def, pred) in &strategies {
        let sp = &scope_pred[def.scope.as_str()].1;
        let (mut is_p, mut is_px, mut oos_p, mut oos_px) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for s in samples.iter().filter(|s| s.horizon == def.horizon) {
            if !sp(s) || !pred(s) {
                continue;
            }
            let y = if s.yes { 1.0 } else { 0.0 };
            let (pnl, px) = match def.side {
                Side::BuyYes => (y - s.ask - fee.fee_per_contract(Fp::from_f64(s.ask), false), s.ask),
                Side::BuyNo => ((1.0 - y) - (1.0 - s.bid) - fee.fee_per_contract(Fp::from_f64(s.bid), false), 1.0 - s.bid),
            };
            if s.close_ts < split_ts {
                is_p.push(pnl);
                is_px.push(px);
            } else {
                oos_p.push(pnl);
                oos_px.push(px);
            }
        }
        if is_p.len() < a.min_n {
            continue;
        }
        let is = stats(&is_p, a.stake, &is_px);
        let oos = stats(&oos_p, a.stake, &oos_px);
        let verdict = if is.t >= 2.0 && is.ev > 0.0 {
            if oos.n >= 30 && oos.t >= 2.0 && oos.ev > 0.0 {
                "PASS"
            } else if oos.n >= 30 {
                "FAIL OOS"
            } else {
                "INCONCLUSIVE"
            }
        } else {
            "NOISE"
        };
        rows.push(Row { def: def.clone(), is, oos, verdict });
    }
    info!(evaluated = rows.len(), secs = t0.elapsed().as_secs_f64(), "universe evaluated");
    rows.sort_by(|x, y| {
        let rank = |r: &Row| match r.verdict { "PASS" => 0, "INCONCLUSIVE" => 1, "FAIL OOS" => 2, _ => 3 };
        rank(x).cmp(&rank(y)).then(y.oos.t.partial_cmp(&x.oos.t).unwrap_or(std::cmp::Ordering::Equal))
    });
    let n_pass = rows.iter().filter(|r| r.verdict == "PASS").count();
    let n_is = rows.iter().filter(|r| r.verdict != "NOISE").count();
    let mut families: HashMap<&str, [usize; 4]> = HashMap::new();
    for r in &rows {
        let e = families.entry(r.def.family).or_default();
        e[0] += 1;
        match r.verdict {
            "PASS" => e[1] += 1,
            "FAIL OOS" => e[2] += 1,
            "INCONCLUSIVE" => e[3] += 1,
            _ => {}
        }
    }
    println!("\nstrategies generated {}  evaluated (n≥{}) {}  passed IS {}  PASSED OOS {}  (expected by luck ≈ {:.0})", strategies.len(), a.min_n, rows.len(), n_is, n_pass, rows.len() as f64 * 0.025 * 0.025);
    println!("{:<14} {:<26} {:>8} {:<34} {:>7} | {:>5} {:>7} {:>6} | {:>5} {:>7} {:>6} {:>9}", "family", "scope", "horizon", "params", "side", "n_is", "ev_is", "t_is", "n_oos", "ev_oos", "t_oos", "verdict");
    for r in rows.iter().take(50) {
        println!(
            "{:<14} {:<26} {:>7}s {:<34} {:>7} | {:>5} {:>+7.3} {:>6.2} | {:>5} {:>+7.3} {:>6.2} {:>9}",
            r.def.family, r.def.scope.chars().take(26).collect::<String>(), r.def.horizon, r.def.params.chars().take(34).collect::<String>(), format!("{:?}", r.def.side), r.is.n, r.is.ev, r.is.t, r.oos.n, r.oos.ev, r.oos.t, r.verdict
        );
    }
    let out = serde_json::json!({
        "kind": "universe", "created_ms": chrono::Utc::now().timestamp_millis(),
        "markets": markets.len(), "samples": samples.len(), "series": series_names.len(), "categories": cat_names,
        "generated": strategies.len(), "evaluated": rows.len(), "passed_is": n_is, "passed_oos": n_pass,
        "expected_false_pass": rows.len() as f64 * 0.025 * 0.025, "split_ts": split_ts, "stake": a.stake,
        "families": families.iter().map(|(k, v)| serde_json::json!({"family": k, "evaluated": v[0], "pass": v[1], "fail_oos": v[2], "inconclusive": v[3]})).collect::<Vec<_>>(),
        "rows": rows.iter().take(3000).collect::<Vec<_>>(),
    });
    std::fs::create_dir_all(&a.out)?;
    let path = a.out.join("universe.json");
    std::fs::write(&path, serde_json::to_vec(&out)?)?;
    info!(path = %path.display(), "universe written");
    Ok(())
}
