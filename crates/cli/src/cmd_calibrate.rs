//! Model-vs-market calibration test. No trading: replay history, and at every
//! Kalshi trade print record (model fair, market price, time-to-expiry) and
//! later the settled outcome. Reports Brier scores and calibration tables so we
//! know whether the model carries information the market doesn't — and at
//! which horizon — before any sizing/fee/fill questions.

use anyhow::Result;
use clap::Args as ClapArgs;
use mb_backtest::history::{load_events, HistoryFilter, RefSource};
use mb_core::{MarketEvent, Outcome};
use mb_strategy::fair_value::prob_above;
use mb_strategy::vol::{ImpliedVol, RealizedVol, VolModel, VolSource};
use mb_strategy::Btc15mConfig;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(ClapArgs, Debug)]
pub struct Args {
    #[arg(long, default_value = "KXBTC15M")]
    pub series: String,
    #[arg(long, default_value = "data")]
    pub data: PathBuf,
    #[arg(long)]
    pub from: Option<String>,
    #[arg(long)]
    pub to: Option<String>,
    #[arg(long, default_value = "strategies/btc15m.toml")]
    pub config: PathBuf,
    /// candles | ticks | both
    #[arg(long, default_value = "both")]
    pub ref_source: String,
    /// Ignore prints closer than this to expiry
    #[arg(long, default_value_t = 75)]
    pub min_tau_secs: i64,
    /// At most one sample per market per this many ms
    #[arg(long, default_value_t = 2000)]
    pub sample_every_ms: i64,
    /// Edge threshold for the "does acting on the signal pay?" table
    #[arg(long, default_value_t = 0.03)]
    pub edge: f64,
    /// Skip samples whose reference price is older than this (the strategy has the same guard)
    #[arg(long, default_value_t = 10_000)]
    pub max_spot_age_ms: i64,
    /// Override config: annualized vol floor
    #[arg(long)]
    pub vol_floor: Option<f64>,
    /// Override config: EWMA lambda
    #[arg(long)]
    pub vol_lambda: Option<f64>,
    /// Override config: variance sample interval (seconds)
    #[arg(long)]
    pub vol_sample_secs: Option<f64>,
    /// Override config: weight on the market price in the blended fair value (0..1)
    #[arg(long)]
    pub blend: Option<f64>,
    /// Override config: realized | implied | max | mean
    #[arg(long)]
    pub vol_source: Option<String>,
}

struct Sample {
    fair: f64,
    px: f64,
    tau: f64,
    spot_age_ms: i64,
    mkt_idx: usize,
}

#[derive(Default)]
struct Bucket {
    n: usize,
    markets: std::collections::HashSet<usize>,
    fair: f64,
    px: f64,
    yes: f64,
    brier_model: f64,
    brier_mkt: f64,
}

impl Bucket {
    fn add(&mut self, s: &Sample, y: f64) {
        self.n += 1;
        self.markets.insert(s.mkt_idx);
        self.fair += s.fair;
        self.px += s.px;
        self.yes += y;
        self.brier_model += (s.fair - y).powi(2);
        self.brier_mkt += (s.px - y).powi(2);
    }
    fn row(&self, label: &str) -> String {
        if self.n == 0 {
            return format!("{label:<12} {:>8} {:>5}", 0, 0);
        }
        let n = self.n as f64;
        format!(
            "{label:<12} {:>8} {:>5} {:>8.3} {:>8.3} {:>8.3} {:>10.4} {:>10.4}",
            self.n,
            self.markets.len(),
            self.fair / n,
            self.px / n,
            self.yes / n,
            self.brier_model / n,
            self.brier_mkt / n
        )
    }
}

pub fn parse_ref_source(s: &str) -> RefSource {
    match s {
        "candles" => RefSource::Candles,
        "ticks" => RefSource::Ticks,
        _ => RefSource::Both,
    }
}

pub async fn run(a: Args) -> Result<()> {
    let mut cfg = if a.config.exists() { Btc15mConfig::load(&a.config)? } else { Btc15mConfig::default() };
    cfg.series = a.series.clone();
    if let Some(v) = a.vol_floor {
        cfg.vol_floor_annual = v;
    }
    if let Some(v) = a.vol_lambda {
        cfg.vol_lambda = v;
    }
    if let Some(v) = a.vol_sample_secs {
        cfg.vol_sample_secs = v;
    }
    if let Some(v) = a.blend {
        cfg.market_blend = v;
    }
    if let Some(v) = &a.vol_source {
        cfg.vol_source = v.clone();
    }
    let filter = HistoryFilter {
        series: Some(a.series.clone()),
        from_ms: a.from.as_deref().map(|s| crate::cmd_backtest::parse_day(s, false)).transpose()?.unwrap_or(0),
        to_ms: a.to.as_deref().map(|s| crate::cmd_backtest::parse_day(s, true)).transpose()?.unwrap_or(i64::MAX),
        ref_symbol: Some(cfg.ref_symbol.clone()),
        ref_source: parse_ref_source(&a.ref_source),
    };
    let events = load_events(&a.data, &filter)?;

    struct Mkt {
        idx: usize,
        strike: f64,
        close_ms: i64,
        samples: Vec<Sample>,
        last_sample_ms: i64,
    }
    let mut mkts: HashMap<String, Mkt> = HashMap::new();
    let mut vol = VolModel::new(
        RealizedVol::new(cfg.vol_lambda, cfg.vol_floor_annual, cfg.vol_cap_annual, (cfg.vol_sample_secs * 1000.0) as i64),
        ImpliedVol::new(cfg.iv_lambda),
        VolSource::parse(&cfg.vol_source),
        cfg.vol_floor_annual,
        cfg.vol_cap_annual,
    );
    let mut basis = mb_strategy::basis::BasisEstimator::new(cfg.settle_avg_secs, cfg.basis_lambda);
    let mut spot: Option<(i64, f64)> = None;
    let mut done: Vec<(Sample, f64)> = Vec::new();
    let mut n_markets = 0usize;

    for ev in &events {
        match ev {
            MarketEvent::Market(m) => {
                if m.series == cfg.series
                    && let Some(k) = m.floor_strike
                    && !mkts.contains_key(&m.ticker)
                {
                    basis.on_market_open(m.open_ts_ms, k);
                    n_markets += 1;
                    mkts.insert(
                        m.ticker.clone(),
                        Mkt {
                            idx: n_markets,
                            strike: k,
                            close_ms: m.close_ts_ms,
                            samples: Vec::new(),
                            last_sample_ms: 0,
                        },
                    );
                }
            }
            MarketEvent::Ref(r) => {
                if r.symbol == cfg.ref_symbol {
                    vol.on_ref(r.ts_ms, r.px);
                    basis.on_ref(r.ts_ms, r.px);
                    spot = Some((r.ts_ms, r.px));
                }
            }
            MarketEvent::Trade(t) => {
                let Some(m) = mkts.get_mut(&t.ticker) else { continue };
                let Some((sts, s)) = spot else { continue };
                let tau = (m.close_ms - t.ts_ms) as f64 / 1000.0;
                let b = if cfg.auto_basis && basis.samples() > 0 { basis.basis() } else { cfg.ref_basis };
                if t.ts_ms - sts <= a.max_spot_age_ms && tau >= 120.0 {
                    // learn implied vol from every fresh print (sample is scored *before* this update)
                    let model_before = prob_above(s + b, m.strike, vol.sigma_per_sec(), tau, cfg.settle_avg_secs);
                    if tau >= a.min_tau_secs as f64 && t.ts_ms - m.last_sample_ms >= a.sample_every_ms {
                        m.last_sample_ms = t.ts_ms;
                        let fair = (1.0 - cfg.market_blend) * model_before + cfg.market_blend * t.yes_px.to_f64();
                        m.samples.push(Sample {
                            fair,
                            px: t.yes_px.to_f64(),
                            tau,
                            spot_age_ms: t.ts_ms - sts,
                            mkt_idx: m.idx,
                        });
                    }
                    vol.on_print(s + b, m.strike, t.yes_px.to_f64(), tau, cfg.settle_avg_secs);
                }
            }
            MarketEvent::Settlement { ticker, result, .. } => {
                if let Some(m) = mkts.remove(ticker) {
                    let y = if *result == Outcome::Yes { 1.0 } else { 0.0 };
                    done.extend(m.samples.into_iter().map(|s| (s, y)));
                }
            }
            _ => {}
        }
    }

    let n = done.len();
    if n == 0 {
        anyhow::bail!("no samples (no settled markets with trades + ref prices in range)");
    }
    let (bmean, bstd) = basis.stats();
    println!(
        "\nsamples {n}  markets {n_markets}  vol(source {}, floor {:.0}%, λ {}, sample {}s) now {:.1}% (implied {}, n={})  blend {}  mean spot age {:.0} ms\nbasis (strike − ref 60s avg): n={} mean ${:.2} std ${:.2}  [{}]",
        cfg.vol_source,
        cfg.vol_floor_annual * 100.0,
        cfg.vol_lambda,
        cfg.vol_sample_secs,
        vol.sigma_annual() * 100.0,
        vol.implied_annual().map(|v| format!("{:.1}%", v * 100.0)).unwrap_or_else(|| "n/a".into()),
        vol.implied.samples(),
        cfg.market_blend,
        done.iter().map(|(s, _)| s.spot_age_ms as f64).sum::<f64>() / n as f64,
        basis.samples(),
        bmean,
        bstd,
        if cfg.auto_basis { "auto-applied" } else { format!("fixed ref_basis={}", cfg.ref_basis).leak() }
    );
    let hdr = format!("{:<12} {:>8} {:>5} {:>8} {:>8} {:>8} {:>10} {:>10}", "bucket", "n", "mkts", "fair", "mkt", "yes%", "brierModel", "brierMkt");
    println!("(n = samples; mkts = independent markets — outcomes within a market are perfectly correlated, judge significance by mkts)");

    // overall + by time-to-expiry
    let mut all = Bucket::default();
    let tau_edges = [75.0, 120.0, 300.0, 600.0, 900.0, f64::MAX];
    let mut by_tau: Vec<Bucket> = (0..tau_edges.len() - 1).map(|_| Bucket::default()).collect();
    let mut by_fair: Vec<Bucket> = (0..10).map(|_| Bucket::default()).collect();
    let mut by_mkt: Vec<Bucket> = (0..10).map(|_| Bucket::default()).collect();
    let mut buy_sig = Bucket::default();
    let mut sell_sig = Bucket::default();
    for (s, y) in &done {
        all.add(s, *y);
        let ti = tau_edges.windows(2).position(|w| s.tau >= w[0] && s.tau < w[1]).unwrap_or(0);
        by_tau[ti].add(s, *y);
        by_fair[((s.fair * 10.0) as usize).min(9)].add(s, *y);
        by_mkt[((s.px * 10.0) as usize).min(9)].add(s, *y);
        if s.fair - s.px > a.edge {
            buy_sig.add(s, *y);
        }
        if s.px - s.fair > a.edge {
            sell_sig.add(s, *y);
        }
    }
    println!("\n== overall ==\n{hdr}\n{}", all.row("all"));
    println!("\n== by seconds to expiry ==\n{hdr}");
    for (i, b) in by_tau.iter().enumerate() {
        let lo = tau_edges[i];
        let hi = tau_edges[i + 1];
        println!("{}", b.row(&if hi == f64::MAX { format!("{lo:.0}s+") } else { format!("{lo:.0}-{hi:.0}s") }));
    }
    println!("\n== calibration by MODEL fair bucket (yes% should track 'fair') ==\n{hdr}");
    for (i, b) in by_fair.iter().enumerate() {
        println!("{}", b.row(&format!("{:.1}-{:.1}", i as f64 / 10.0, (i + 1) as f64 / 10.0)));
    }
    println!("\n== calibration by MARKET price bucket (yes% should track 'mkt') ==\n{hdr}");
    for (i, b) in by_mkt.iter().enumerate() {
        println!("{}", b.row(&format!("{:.1}-{:.1}", i as f64 / 10.0, (i + 1) as f64 / 10.0)));
    }
    println!("\n== when the model disagrees with the market by > {:.2} ==\n{hdr}", a.edge);
    println!("{}", buy_sig.row("BUY YES sig"));
    println!("{}", sell_sig.row("BUY NO sig"));
    if buy_sig.n > 0 {
        let n = buy_sig.n as f64;
        println!("  buying YES at mkt would earn {:+.4}/contract before fees", buy_sig.yes / n - buy_sig.px / n);
    }
    if sell_sig.n > 0 {
        let n = sell_sig.n as f64;
        println!("  buying NO  at mkt would earn {:+.4}/contract before fees", sell_sig.px / n - sell_sig.yes / n);
    }
    Ok(())
}
