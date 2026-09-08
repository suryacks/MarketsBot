use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use mb_backtest::history::{load_events, HistoryFilter, RefSource};
use mb_backtest::{Backtester, FillMode, Report, SimConfig, SimExchange};
use mb_core::{FeeModel, Fp, MarketEvent};
use mb_strategy::{Btc15mConfig, Btc15mStrategy, SpreadMaker, SpreadMakerConfig};
use std::path::{Path, PathBuf};
use tracing::info;

#[derive(ClapArgs, Debug)]
pub struct Args {
    #[arg(long, default_value = "KXBTC15M")]
    pub series: String,
    #[arg(long, default_value = "data")]
    pub data: PathBuf,
    /// Start date (UTC, YYYY-MM-DD). Default: everything on disk.
    #[arg(long)]
    pub from: Option<String>,
    /// End date (UTC, YYYY-MM-DD, inclusive)
    #[arg(long)]
    pub to: Option<String>,
    /// Strategy config TOML (defaults are used if absent)
    #[arg(long, default_value = "strategies/btc15m.toml")]
    pub config: PathBuf,
    /// btc15m | spread-maker
    #[arg(long, default_value = "btc15m")]
    pub strategy: String,
    /// Simulated order latency (submit → active), ms
    #[arg(long, default_value_t = 250)]
    pub latency_ms: i64,
    /// Tape mode: how long an inferred touch stays valid, ms
    #[arg(long, default_value_t = 2000)]
    pub touch_ttl_ms: i64,
    /// tape | book
    #[arg(long, default_value = "tape")]
    pub mode: String,
    /// candles | ticks | both — which reference-price data to replay
    #[arg(long, default_value = "both")]
    pub ref_source: String,
    #[arg(long, default_value_t = 100.0)]
    pub bankroll: f64,
    /// Kalshi series fee multiplier (1.0 = standard 7% quadratic taker fee)
    #[arg(long, default_value_t = 1.0)]
    pub fee_multiplier: f64,
    /// Comma-separated min_edge values to sweep (overrides config)
    #[arg(long)]
    pub edges: Option<String>,
    /// Override config: maker mode (rest post-only quotes) instead of taking
    #[arg(long)]
    pub maker: bool,
    /// Override config: minimum seconds to expiry to trade
    #[arg(long)]
    pub min_tau_secs: Option<i64>,
    /// Override config: realized | implied | max | mean
    #[arg(long)]
    pub vol_source: Option<String>,
    /// Override config: weight on market mid in the blended fair value
    #[arg(long)]
    pub blend: Option<f64>,
    /// Override config: max taker entries per market
    #[arg(long)]
    pub max_entries: Option<u32>,
    /// Probability a resting order fills when the tape prints at (not through) its price
    #[arg(long, default_value_t = 0.5)]
    pub maker_touch_fill_prob: f64,
    /// Where to write CSV reports
    #[arg(long, default_value = "reports")]
    pub report: PathBuf,
}

pub fn parse_day(s: &str, end: bool) -> Result<i64> {
    let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").with_context(|| format!("bad date {s}"))?;
    let t = if end { d.and_hms_opt(23, 59, 59).unwrap() } else { d.and_hms_opt(0, 0, 0).unwrap() };
    Ok(t.and_utc().timestamp_millis())
}

/// Everything a single backtest needs — used by the CLI and by the strategy lab.
#[derive(Clone, Debug)]
pub struct BacktestSpec {
    pub name: String,
    pub strategy: String,
    pub series: Vec<String>,
    pub data: PathBuf,
    pub from: Option<String>,
    pub to: Option<String>,
    pub config: PathBuf,
    pub mode: FillMode,
    pub ref_source: RefSource,
    pub ref_symbol: Option<String>,
    pub latency_ms: i64,
    pub touch_ttl_ms: i64,
    pub bankroll: f64,
    pub fee_multiplier: f64,
    pub edge: Option<f64>,
    pub maker: bool,
    pub min_tau_secs: Option<i64>,
    pub vol_source: Option<String>,
    pub blend: Option<f64>,
    pub max_entries: Option<u32>,
    pub maker_touch_fill_prob: f64,
}

pub struct BacktestOutcome {
    pub report: Report,
    pub params: serde_json::Value,
    #[allow(dead_code)]
    pub events: usize,
}

/// Load events once for a spec (so sweeps can reuse them).
pub fn load_spec_events(spec: &BacktestSpec) -> Result<Vec<MarketEvent>> {
    let filter = HistoryFilter {
        series: if spec.series.len() == 1 { Some(spec.series[0].clone()) } else { None },
        from_ms: spec.from.as_deref().map(|s| parse_day(s, false)).transpose()?.unwrap_or(0),
        to_ms: spec.to.as_deref().map(|s| parse_day(s, true)).transpose()?.unwrap_or(i64::MAX),
        ref_symbol: spec.ref_symbol.clone(),
        ref_source: spec.ref_source,
    };
    let mut events = load_events(&spec.data, &filter)?;
    if spec.series.len() > 1 {
        let keep: std::collections::HashSet<&str> = spec.series.iter().map(|s| s.as_str()).collect();
        events.retain(|e| match e {
            MarketEvent::Market(m) => keep.contains(m.series.as_str()),
            other => other.ticker().map(|t| keep.contains(t.split('-').next().unwrap_or(""))).unwrap_or(true),
        });
    }
    Ok(events)
}

pub fn run_spec(spec: &BacktestSpec, events: &[MarketEvent]) -> Result<BacktestOutcome> {
    if events.is_empty() {
        anyhow::bail!("no events for {} — fetch history first", spec.series.join(","));
    }
    let sim = SimExchange::new(SimConfig {
        mode: spec.mode,
        latency_ms: spec.latency_ms,
        touch_ttl_ms: spec.touch_ttl_ms,
        initial_cash: Fp::from_f64(spec.bankroll),
        default_fee: FeeModel::kalshi("quadratic", spec.fee_multiplier),
        maker_touch_fill_prob: spec.maker_touch_fill_prob,
    });
    let (strat, params): (Box<dyn mb_core::Strategy>, serde_json::Value) = match spec.strategy.as_str() {
        "spread-maker" | "spread_maker" => {
            let mut cfg = if spec.config.exists() { SpreadMakerConfig::load(&spec.config)? } else { SpreadMakerConfig::default() };
            cfg.series = spec.series.clone();
            if let Some(e) = spec.edge {
                cfg.half_spread = e;
            }
            cfg.scale_to_bankroll(spec.bankroll);
            let p = serde_json::json!({"half_spread": cfg.half_spread, "quote_qty": cfg.quote_qty, "max_inventory": cfg.max_inventory});
            (Box::new(SpreadMaker::new(cfg)), p)
        }
        _ => {
            let mut cfg = if spec.config.exists() { Btc15mConfig::load(&spec.config)? } else { Btc15mConfig::default() };
            cfg.series = spec.series[0].clone();
            if let Some(r) = &spec.ref_symbol {
                cfg.ref_symbol = r.clone();
            }
            if let Some(e) = spec.edge {
                cfg.min_edge = e;
            }
            cfg.maker = spec.maker;
            if let Some(t) = spec.min_tau_secs {
                cfg.min_tau_secs = t;
            }
            if let Some(v) = &spec.vol_source {
                cfg.vol_source = v.clone();
            }
            if let Some(b) = spec.blend {
                cfg.market_blend = b;
            }
            if let Some(m) = spec.max_entries {
                cfg.max_entries_per_market = m;
            }
            cfg.scale_to_bankroll(spec.bankroll);
            let p = serde_json::json!({"edge": cfg.min_edge, "maker": cfg.maker, "min_tau_secs": cfg.min_tau_secs, "vol_source": cfg.vol_source, "blend": cfg.market_blend,
                                       "max_entries": cfg.max_entries_per_market, "max_contracts_per_market": cfg.max_contracts_per_market, "maker_qty": cfg.maker_qty});
            (Box::new(Btc15mStrategy::new(cfg)), p)
        }
    };
    let mut bt = Backtester::new(sim, strat);
    bt.run(events);
    let report = bt.report(Fp::from_f64(spec.bankroll));
    let mut params = params;
    params["name"] = serde_json::json!(spec.name);
    params["strategy"] = serde_json::json!(spec.strategy);
    params["series"] = serde_json::json!(spec.series.join(","));
    params["from"] = serde_json::json!(spec.from);
    params["to"] = serde_json::json!(spec.to);
    params["mode"] = serde_json::json!(if spec.mode == FillMode::Book { "book" } else { "tape" });
    params["ref_source"] = serde_json::json!(format!("{:?}", spec.ref_source).to_lowercase());
    params["latency_ms"] = serde_json::json!(spec.latency_ms);
    params["bankroll"] = serde_json::json!(spec.bankroll);
    params["maker_touch_fill_prob"] = serde_json::json!(spec.maker_touch_fill_prob);
    if spec.maker {
        let qs = &bt.sim.queue_stats;
        params["queue"] = serde_json::json!({"rested": qs.orders_rested, "avg_ahead": if qs.orders_rested > 0 { qs.ahead_at_insert / qs.orders_rested as f64 } else { 0.0 }, "reached_front": qs.reached_front, "fills_at_price": qs.fills_at_price, "fills_through": qs.fills_through});
    }
    Ok(BacktestOutcome {
        report,
        params,
        events: events.len(),
    })
}

pub fn parse_ref_source(s: &str) -> RefSource {
    match s {
        "candles" => RefSource::Candles,
        "ticks" => RefSource::Ticks,
        _ => RefSource::Both,
    }
}

pub fn write_outcome(dir: &Path, tag: &str, out: &BacktestOutcome, fills_csv: bool) -> Result<()> {
    out.report.write_csv(dir.join(format!("{tag}-markets.csv")))?;
    out.report.write_json(dir.join(format!("{tag}.json")), out.params.clone())?;
    let _ = fills_csv;
    Ok(())
}

pub async fn run(a: Args) -> Result<()> {
    let ref_symbol = if a.strategy == "btc15m" {
        Some(if a.config.exists() { Btc15mConfig::load(&a.config)?.ref_symbol } else { Btc15mConfig::default().ref_symbol })
    } else {
        None
    };
    let edges: Vec<Option<f64>> = match &a.edges {
        Some(s) => s.split(',').filter_map(|x| x.trim().parse().ok()).map(Some).collect(),
        None => vec![None],
    };
    let base = BacktestSpec {
        name: String::new(),
        strategy: a.strategy.clone(),
        series: vec![a.series.clone()],
        data: a.data.clone(),
        from: a.from.clone(),
        to: a.to.clone(),
        config: a.config.clone(),
        mode: if a.mode == "book" { FillMode::Book } else { FillMode::Tape },
        ref_source: parse_ref_source(&a.ref_source),
        ref_symbol,
        latency_ms: a.latency_ms,
        touch_ttl_ms: a.touch_ttl_ms,
        bankroll: a.bankroll,
        fee_multiplier: a.fee_multiplier,
        edge: None,
        maker: a.maker,
        min_tau_secs: a.min_tau_secs,
        vol_source: a.vol_source.clone(),
        blend: a.blend,
        max_entries: a.max_entries,
        maker_touch_fill_prob: a.maker_touch_fill_prob,
    };
    let events = load_spec_events(&base)?;
    let mut summary = Vec::new();
    for edge in edges {
        let mut spec = base.clone();
        spec.edge = edge;
        let e = edge.unwrap_or(f64::NAN);
        spec.name = format!(
            "{}-{}-{}-{}-{}-edge{:.3}-lat{}-b{}-{}",
            a.series,
            a.from.clone().unwrap_or_else(|| "all".into()),
            a.to.clone().unwrap_or_else(|| "all".into()),
            a.strategy,
            if a.maker { format!("maker-q{}", a.maker_touch_fill_prob) } else { "taker".into() },
            e,
            a.latency_ms,
            a.blend.unwrap_or(0.0),
            a.vol_source.clone().unwrap_or_else(|| "realized".into())
        );
        let out = run_spec(&spec, &events)?;
        println!(
            "\n=== {} | {} ===\n{}",
            spec.name,
            serde_json::to_string(&out.params).unwrap_or_default(),
            out.report.summary()
        );
        if let Some(q) = out.params.get("queue") {
            println!("queue: {q}");
        }
        write_outcome(&a.report, &spec.name, &out, true)?;
        let r = &out.report;
        summary.push((e, r.markets_traded, r.n_fills, r.net_pnl, r.fees, r.win_rate, r.max_drawdown, r.pnl_per_contract, r.t_stat));
    }
    if summary.len() > 1 {
        println!("\nedge    mkts  fills   net_pnl     fees   win%   maxDD   pnl/ct      t");
        for (e, m, f, pnl, fees, wr, dd, ppc, t) in summary {
            println!("{e:<7.3} {m:<5} {f:<6} {pnl:>9.2} {fees:>8.2} {:>5.1} {dd:>7.2} {ppc:>8.4} {t:>6.2}", wr * 100.0);
        }
    }
    info!(dir = %a.report.display(), "reports written");
    Ok(())
}
