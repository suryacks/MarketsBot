use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use mb_backtest::history::{load_events, HistoryFilter};
use mb_backtest::{Backtester, FillMode, SimConfig, SimExchange};
use mb_core::{FeeModel, Fp};
use mb_strategy::{Btc15mConfig, Btc15mStrategy};
use std::path::PathBuf;
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
    #[arg(long, default_value_t = 1000.0)]
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

pub async fn run(a: Args) -> Result<()> {
    let mut cfg = if a.config.exists() {
        Btc15mConfig::load(&a.config)?
    } else {
        info!(path = %a.config.display(), "config not found, using defaults");
        Btc15mConfig::default()
    };
    cfg.series = a.series.clone();
    if a.maker {
        cfg.maker = true;
    }
    if let Some(t) = a.min_tau_secs {
        cfg.min_tau_secs = t;
    }
    if let Some(v) = &a.vol_source {
        cfg.vol_source = v.clone();
    }
    if let Some(v) = a.blend {
        cfg.market_blend = v;
    }
    if let Some(v) = a.max_entries {
        cfg.max_entries_per_market = v;
    }

    let filter = HistoryFilter {
        series: Some(a.series.clone()),
        from_ms: a.from.as_deref().map(|s| parse_day(s, false)).transpose()?.unwrap_or(0),
        to_ms: a.to.as_deref().map(|s| parse_day(s, true)).transpose()?.unwrap_or(i64::MAX),
        ref_symbol: Some(cfg.ref_symbol.clone()),
        ref_source: crate::cmd_calibrate::parse_ref_source(&a.ref_source),
    };
    let events = load_events(&a.data, &filter)?;
    if events.is_empty() {
        anyhow::bail!("no events loaded — run `mbot fetch-history --series {}` first", a.series);
    }

    let edges: Vec<f64> = match &a.edges {
        Some(s) => s.split(',').filter_map(|x| x.trim().parse().ok()).collect(),
        None => vec![cfg.min_edge],
    };
    let mode = match a.mode.as_str() {
        "book" => FillMode::Book,
        _ => FillMode::Tape,
    };

    let mut summary_rows = Vec::new();
    for edge in edges {
        let mut c = cfg.clone();
        c.min_edge = edge;
        let sim = SimExchange::new(SimConfig {
            mode,
            latency_ms: a.latency_ms,
            touch_ttl_ms: a.touch_ttl_ms,
            initial_cash: Fp::from_f64(a.bankroll),
            default_fee: FeeModel::kalshi("quadratic", a.fee_multiplier),
            maker_touch_fill_prob: a.maker_touch_fill_prob,
        });
        let strat = Btc15mStrategy::new(c);
        let mut bt = Backtester::new(sim, Box::new(strat));
        bt.run(&events);
        let report = bt.report(Fp::from_f64(a.bankroll));
        println!(
            "\n=== min_edge = {edge:.3} | latency {} ms | {} fills | {} | min_tau {}s | vol {} | blend {} | max_entries {} ===\n{}",
            a.latency_ms,
            a.mode,
            if cfg.maker { "MAKER" } else { "TAKER" },
            cfg.min_tau_secs,
            cfg.vol_source,
            cfg.market_blend,
            cfg.max_entries_per_market,
            report.summary()
        );
        let tag = format!(
            "{}-{}-{}-{}-edge{:.3}-lat{}",
            a.series,
            a.from.clone().unwrap_or_else(|| "all".into()),
            a.to.clone().unwrap_or_else(|| "all".into()),
            if cfg.maker { "maker" } else { "taker" },
            edge,
            a.latency_ms
        );
        report.write_csv(a.report.join(format!("{tag}-markets.csv")))?;
        mb_backtest::Report::write_fills_csv(a.report.join(format!("{tag}-fills.csv")), bt.fills())?;
        summary_rows.push((edge, report.markets_traded, report.n_fills, report.net_pnl, report.fees, report.win_rate, report.max_drawdown, report.pnl_per_contract));
    }
    if summary_rows.len() > 1 {
        println!("\nedge    mkts  fills   net_pnl     fees   win%   maxDD   pnl/ct");
        for (e, m, f, pnl, fees, wr, dd, ppc) in summary_rows {
            println!("{e:<7.3} {m:<5} {f:<6} {pnl:>9.2} {fees:>8.2} {:>5.1} {dd:>7.2} {ppc:>8.4}", wr * 100.0);
        }
    }
    info!(dir = %a.report.display(), "reports written");
    Ok(())
}
