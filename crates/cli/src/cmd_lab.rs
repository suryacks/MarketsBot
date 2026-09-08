//! Strategy lab: run a matrix of (strategy × market × window) backtests on
//! historical data, fetching missing history on the way, and write each
//! result as it lands to `reports/lab/<name>.json` for the dashboard.
//! Verdicts: PASS = net > 0 && t ≥ 2 && ≥ 100 markets; FAIL = ≥ 100 markets and
//! (net ≤ 0 or t < 1); otherwise INCONCLUSIVE (needs more data).

use crate::cmd_backtest::{load_spec_events, parse_ref_source, run_spec, write_outcome, BacktestSpec};
use anyhow::{Context, Result};
use clap::Args as ClapArgs;
use mb_backtest::FillMode;
use serde::Deserialize;
use serde_json::json;
use std::path::PathBuf;
use tracing::{error, info, warn};

#[derive(ClapArgs, Debug)]
pub struct Args {
    #[arg(long, default_value = "strategies/lab.toml")]
    pub config: PathBuf,
    #[arg(long, default_value = "reports/lab")]
    pub out: PathBuf,
    /// Only run experiments whose name contains this
    #[arg(long)]
    pub filter: Option<String>,
    /// Re-run experiments that already have a result
    #[arg(long)]
    pub force: bool,
    /// Skip data fetching (fail experiments whose data is missing)
    #[arg(long)]
    pub no_fetch: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Experiment {
    pub name: String,
    /// btc15m | spread-maker
    pub strategy: String,
    pub series: Vec<String>,
    /// Coinbase product for reference ticks (btc15m only)
    pub ref_product: String,
    pub data: String,
    pub days: i64,
    pub from: Option<String>,
    pub to: Option<String>,
    pub config: String,
    /// tape | book
    pub mode: String,
    pub ref_source: String,
    pub latency_ms: i64,
    pub bankroll: f64,
    pub edges: Vec<f64>,
    pub maker: bool,
    pub blend: Option<f64>,
    pub vol_source: Option<String>,
    pub min_tau_secs: Option<i64>,
    pub max_entries: Option<u32>,
    pub maker_touch_fill_prob: f64,
    pub endgame: bool,
    pub description: String,
}

impl Default for Experiment {
    fn default() -> Self {
        Self {
            name: String::new(),
            strategy: "btc15m".into(),
            series: vec![],
            ref_product: "BTC-USD".into(),
            data: "data".into(),
            days: 10,
            from: None,
            to: None,
            config: "strategies/btc15m.toml".into(),
            mode: "tape".into(),
            ref_source: "ticks".into(),
            latency_ms: 250,
            bankroll: 100.0,
            edges: vec![0.05],
            maker: false,
            blend: None,
            vol_source: None,
            min_tau_secs: None,
            max_entries: None,
            maker_touch_fill_prob: 0.25,
            endgame: false,
            description: String::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct LabConfig {
    pub experiment: Vec<Experiment>,
}

fn verdict(r: &mb_backtest::Report) -> &'static str {
    let wiped = r.initial_cash > 0.0 && r.net_pnl <= -0.9 * r.initial_cash;
    if wiped {
        "FAIL"
    } else if r.markets_traded >= 100 && r.net_pnl > 0.0 && r.t_stat >= 2.0 {
        "PASS"
    } else if r.markets_traded >= 100 && (r.net_pnl <= 0.0 || r.t_stat < 1.0) {
        "FAIL"
    } else {
        "INCONCLUSIVE"
    }
}

fn write(out: &PathBuf, name: &str, v: &serde_json::Value) -> Result<()> {
    std::fs::create_dir_all(out)?;
    let p = out.join(format!("{name}.json"));
    let tmp = p.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(v)?)?;
    std::fs::rename(tmp, p)?;
    Ok(())
}

pub async fn run(a: Args) -> Result<()> {
    let cfg: LabConfig = toml::from_str(&std::fs::read_to_string(&a.config).with_context(|| format!("reading {}", a.config.display()))?)?;
    let exps: Vec<Experiment> = cfg
        .experiment
        .into_iter()
        .filter(|e| a.filter.as_ref().map(|f| e.name.contains(f.as_str())).unwrap_or(true))
        .collect();
    info!(n = exps.len(), "lab experiments");
    let started = chrono::Utc::now().timestamp_millis();

    for e in &exps {
        let path = a.out.join(format!("{}.json", e.name));
        if path.exists() && !a.force {
            if let Ok(s) = std::fs::read(&path)
                && let Ok(v) = serde_json::from_slice::<serde_json::Value>(&s)
                && v["status"] == "done"
            {
                info!(name = %e.name, "already done (use --force to rerun)");
                continue;
            }
        }
        let base = json!({"name": e.name, "strategy": e.strategy, "series": e.series, "description": e.description, "mode": e.mode,
                          "days": e.days, "bankroll": e.bankroll, "maker": e.maker, "blend": e.blend, "vol_source": e.vol_source,
                          "lab_started_ms": started, "updated_ms": chrono::Utc::now().timestamp_millis()});
        let mut v = base.clone();
        v["status"] = json!("running");
        v["stage"] = json!("fetching data");
        write(&a.out, &e.name, &v)?;

        // data: fetch if needed
        let data = PathBuf::from(&e.data);
        if !a.no_fetch && (e.strategy == "btc15m" || e.strategy.starts_with("flow")) {
            for s in &e.series {
                let have = data.join("trades").join(s).exists();
                let have_refs = data.join("refs").join(&e.ref_product).exists();
                if !have || !have_refs {
                    info!(series = s, "fetching history for lab");
                    let r = crate::cmd_history::run(crate::cmd_history::Args {
                        series: s.clone(),
                        days: e.days,
                        out: data.clone(),
                        ref_product: e.ref_product.clone(),
                        concurrency: 4,
                        force: false,
                        ref_ticks: true,
                        refs_only: false,
                    })
                    .await;
                    if let Err(err) = r {
                        error!(series = s, error = %err, "fetch failed");
                    }
                }
            }
        }
        v["stage"] = json!("backtesting");
        v["updated_ms"] = json!(chrono::Utc::now().timestamp_millis());
        write(&a.out, &e.name, &v)?;

        let (from, to) = if e.from.is_some() || e.to.is_some() {
            (e.from.clone(), e.to.clone())
        } else {
            let now = chrono::Utc::now().date_naive();
            (Some((now - chrono::Duration::days(e.days)).to_string()), Some(now.to_string()))
        };
        let spec = BacktestSpec {
            name: e.name.clone(),
            strategy: e.strategy.clone(),
            series: e.series.clone(),
            data: data.clone(),
            from,
            to,
            config: PathBuf::from(&e.config),
            mode: if e.mode == "book" { FillMode::Book } else { FillMode::Tape },
            ref_source: parse_ref_source(&e.ref_source),
            ref_symbol: if e.strategy == "btc15m" || e.strategy.starts_with("flow") { Some(e.ref_product.clone()) } else { None },
            latency_ms: e.latency_ms,
            touch_ttl_ms: 2000,
            bankroll: e.bankroll,
            fee_multiplier: 1.0,
            edge: None,
            maker: e.maker,
            min_tau_secs: e.min_tau_secs,
            vol_source: e.vol_source.clone(),
            blend: e.blend,
            max_entries: e.max_entries,
            maker_touch_fill_prob: e.maker_touch_fill_prob,
            endgame: e.endgame,
            invert: false,
        };
        let events = match load_spec_events(&spec) {
            Ok(ev) => ev,
            Err(err) => {
                v["status"] = json!("error");
                v["error"] = json!(err.to_string());
                write(&a.out, &e.name, &v)?;
                continue;
            }
        };
        let mut sweeps = Vec::new();
        let mut best: Option<(f64, serde_json::Value)> = None;
        for edge in if e.edges.is_empty() { vec![f64::NAN] } else { e.edges.clone() } {
            let mut s = spec.clone();
            if !edge.is_nan() {
                s.edge = Some(edge);
            }
            match run_spec(&s, &events) {
                Ok(out) => {
                    let r = &out.report;
                    let row = json!({"edge": edge, "markets_traded": r.markets_traded, "markets_seen": r.markets_seen, "n_fills": r.n_fills,
                                     "net_pnl": r.net_pnl, "fees": r.fees, "win_rate": r.win_rate, "pnl_per_contract": r.pnl_per_contract,
                                     "max_drawdown": r.max_drawdown, "t_stat": r.t_stat, "verdict": verdict(r), "queue": out.params.get("queue").cloned()});
                    let tag = format!("lab-{}-edge{:.3}", e.name, edge);
                    let _ = write_outcome(&PathBuf::from("reports"), &tag, &out, false);
                    let score = if r.markets_traded >= 30 { r.t_stat } else { f64::NEG_INFINITY };
                    if best.as_ref().map(|(b, _)| score > *b).unwrap_or(true) {
                        best = Some((score, row.clone()));
                    }
                    sweeps.push(row);
                    info!(name = %e.name, edge, net = r.net_pnl, t = r.t_stat, markets = r.markets_traded, verdict = verdict(r), "lab result");
                }
                Err(err) => warn!(name = %e.name, edge, error = %err, "backtest failed"),
            }
        }
        v["status"] = json!("done");
        v["stage"] = json!("backtest");
        v["events"] = json!(events.len());
        v["sweeps"] = json!(sweeps);
        v["best"] = best.map(|(_, r)| r).unwrap_or(json!(null));
        v["verdict"] = v["best"]["verdict"].clone();
        // A maker strategy scored on the trade tape depends on an assumed queue-fill probability that
        // live books have already shown to be optimistic; it cannot pass on tape data alone.
        if e.maker && e.mode != "book" && v["verdict"] == "PASS" {
            v["verdict"] = json!("NEEDS BOOK DATA");
            v["note"] = json!("maker fills on the trade tape assume a queue-fill probability; live books show ~3k contracts ahead of each quote — confirm with mode = \"book\" on recorded books (paper run is doing this)");
        }
        v["updated_ms"] = json!(chrono::Utc::now().timestamp_millis());
        write(&a.out, &e.name, &v)?;
    }
    info!("lab complete");
    Ok(())
}
