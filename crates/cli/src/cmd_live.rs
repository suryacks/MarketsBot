//! Live data plumbing shared by `collect` and `paper`.
//!
//! Sources (each a tokio task pushing `MarketEvent`s into one channel):
//! * Kalshi market discovery: polls open markets of a series, emits `Market`
//!   events for new tickers, publishes the ticker set to the WS task, and
//!   detects settlement.
//! * Kalshi books/trades: WebSocket when API keys are configured; otherwise a
//!   1 Hz REST poll of the public markets endpoint (touch-only book) + trades.
//! * Coinbase ticker WebSocket for reference prices.
//! * Polymarket market WebSocket for any token ids given.

use anyhow::Result;
use clap::Args as ClapArgs;
use mb_backtest::{Backtester, FillMode, SimConfig, SimExchange};
use mb_coinbase::CoinbaseWs;
use mb_core::{FeeModel, Fp, MarketEvent, Venue};
use mb_data::Recorder;
use mb_kalshi::{Channel, KalshiClient, KalshiWs, MarketsQuery};
use mb_polymarket::PolymarketWs;
use mb_strategy::{Btc15mConfig, Btc15mStrategy};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

#[derive(ClapArgs, Debug, Clone)]
pub struct FeedArgs {
    /// Kalshi series to follow (repeatable)
    #[arg(long = "series", default_values_t = vec!["KXBTC15M".to_string()])]
    pub series: Vec<String>,
    /// Coinbase products for reference prices (repeatable; empty to disable)
    #[arg(long = "coinbase", default_values_t = vec!["BTC-USD".to_string()])]
    pub coinbase: Vec<String>,
    /// Polymarket CLOB token ids to stream (repeatable)
    #[arg(long = "poly-token")]
    pub poly_tokens: Vec<String>,
    /// Market discovery interval, seconds
    #[arg(long, default_value_t = 10)]
    pub discover_secs: u64,
    /// REST polling interval when no Kalshi API keys are configured, ms
    #[arg(long, default_value_t = 1000)]
    pub poll_ms: u64,
}

#[derive(ClapArgs, Debug)]
pub struct CollectArgs {
    #[command(flatten)]
    pub feed: FeedArgs,
    #[arg(long, default_value = "data/live")]
    pub out: PathBuf,
}

#[derive(ClapArgs, Debug)]
pub struct PaperArgs {
    #[command(flatten)]
    pub feed: FeedArgs,
    #[arg(long, default_value = "strategies/btc15m.toml")]
    pub config: PathBuf,
    #[arg(long, default_value_t = 1000.0)]
    pub bankroll: f64,
    #[arg(long, default_value_t = 250)]
    pub latency_ms: i64,
    /// Also record everything seen to this directory
    #[arg(long)]
    pub record: Option<PathBuf>,
}

pub struct Feeds {
    pub rx: mpsc::Receiver<MarketEvent>,
    pub fee_models: HashMap<String, FeeModel>,
    pub authenticated: bool,
}

pub async fn start_feeds(a: &FeedArgs) -> Result<Feeds> {
    let (tx, rx) = mpsc::channel::<MarketEvent>(65_536);
    let client = KalshiClient::from_env()?;
    let authenticated = client.is_authenticated();
    info!(base = client.base(), authenticated, "kalshi client");

    // per-series fee models
    let mut fee_models = HashMap::new();
    for s in &a.series {
        match client.get_series(s).await {
            Ok(sr) => {
                let fm = FeeModel::kalshi(sr.fee_type.as_deref().unwrap_or("quadratic"), sr.fee_multiplier.unwrap_or(1.0));
                info!(series = s, fee_type = ?sr.fee_type, mult = ?sr.fee_multiplier, "fee model");
                fee_models.insert(s.clone(), fm);
            }
            Err(e) => warn!(series = s, error = %e, "could not load series; using default fees"),
        }
    }

    // Kalshi: discovery + (ws | poll)
    let (wtx, wrx) = watch::channel::<Vec<String>>(Vec::new());
    if authenticated {
        let ws = KalshiWs::from_env()?;
        let txc = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = ws.run_dynamic(&[Channel::OrderbookDelta, Channel::Trade, Channel::Ticker], wrx, txc).await {
                warn!(error = %e, "kalshi ws task ended");
            }
        });
    }
    {
        let client = client.clone();
        let series = a.series.clone();
        let txc = tx.clone();
        let discover = Duration::from_secs(a.discover_secs);
        let poll = Duration::from_millis(a.poll_ms);
        tokio::spawn(async move {
            if let Err(e) = discovery_loop(client, series, txc, wtx, authenticated, discover, poll).await {
                warn!(error = %e, "kalshi discovery task ended");
            }
        });
    }

    // Coinbase
    if !a.coinbase.is_empty() {
        let products = a.coinbase.clone();
        let txc = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = CoinbaseWs::run(products, txc).await {
                warn!(error = %e, "coinbase task ended");
            }
        });
    }

    // Polymarket
    if !a.poly_tokens.is_empty() {
        let tokens = a.poly_tokens.clone();
        let txc = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = PolymarketWs::new().run(tokens, txc).await {
                warn!(error = %e, "polymarket task ended");
            }
        });
    }

    Ok(Feeds {
        rx,
        fee_models,
        authenticated,
    })
}

async fn discovery_loop(
    client: KalshiClient,
    series: Vec<String>,
    tx: mpsc::Sender<MarketEvent>,
    wtx: watch::Sender<Vec<String>>,
    authenticated: bool,
    discover_every: Duration,
    poll_every: Duration,
) -> Result<()> {
    let mut known: HashMap<String, mb_core::MarketInfo> = HashMap::new();
    let mut settled: HashSet<String> = HashSet::new();
    let mut last_trade_ts: HashMap<String, i64> = HashMap::new();
    let mut seen_trade_ids: HashSet<String> = HashSet::new();
    let mut last_discovery = std::time::Instant::now() - discover_every;
    let mut last_poll = std::time::Instant::now();

    loop {
        // ---- discovery ----
        if last_discovery.elapsed() >= discover_every {
            last_discovery = std::time::Instant::now();
            let mut open_tickers = Vec::new();
            for s in &series {
                let q = MarketsQuery {
                    series_ticker: Some(s.clone()),
                    status: Some("open".into()),
                    ..Default::default()
                };
                match client.get_all_markets(&q).await {
                    Ok(ms) => {
                        for m in ms {
                            let info = m.to_info();
                            open_tickers.push(info.ticker.clone());
                            let is_new = !known.contains_key(&info.ticker);
                            if is_new || known.get(&info.ticker).is_some_and(|k| k.close_ts_ms != info.close_ts_ms) {
                                if is_new {
                                    info!(ticker = %info.ticker, strike = ?info.floor_strike, close = info.close_ts_ms, "new market");
                                }
                                known.insert(info.ticker.clone(), info.clone());
                                let _ = tx.send(MarketEvent::Market(info)).await;
                            }
                        }
                    }
                    Err(e) => warn!(series = s, error = %e, "discovery failed"),
                }
            }
            let _ = wtx.send(open_tickers);

            // ---- settlement detection for markets past close ----
            let now = chrono::Utc::now().timestamp_millis();
            let past: Vec<String> = known
                .iter()
                .filter(|(t, m)| m.close_ts_ms > 0 && now > m.close_ts_ms + 5_000 && !settled.contains(*t))
                .map(|(t, _)| t.clone())
                .collect();
            for t in past {
                match client.get_market(&t).await {
                    Ok(m) => {
                        let info = m.to_info();
                        if let Some(r) = info.settled_outcome() {
                            info!(ticker = %t, result = ?r, value = ?info.settlement_value, "settled");
                            settled.insert(t.clone());
                            let _ = tx.send(MarketEvent::Market(info.clone())).await;
                            let _ = tx
                                .send(MarketEvent::Settlement {
                                    venue: Venue::Kalshi,
                                    ticker: t.clone(),
                                    ts_ms: now,
                                    result: r,
                                })
                                .await;
                            known.remove(&t);
                            last_trade_ts.remove(&t);
                        } else if now > info.close_ts_ms + 30 * 60_000 {
                            warn!(ticker = %t, status = %info.status, "not settled 30 min after close; dropping");
                            settled.insert(t.clone());
                            known.remove(&t);
                        }
                    }
                    Err(e) => warn!(ticker = %t, error = %e, "settlement check failed"),
                }
            }
        }

        // ---- unauthenticated fallback: poll touch + trades ----
        if !authenticated && last_poll.elapsed() >= poll_every {
            last_poll = std::time::Instant::now();
            let tickers: Vec<String> = known.keys().cloned().collect();
            if !tickers.is_empty() {
                let q = MarketsQuery {
                    tickers: Some(tickers.clone()),
                    ..Default::default()
                };
                if let Ok(r) = client.get_markets(&q, None).await {
                    let now = chrono::Utc::now().timestamp_millis();
                    for m in r.markets {
                        let mut bids = Vec::new();
                        let mut asks = Vec::new();
                        if let (Some(p), Some(q)) = (m.yes_bid_dollars, m.yes_bid_size_fp)
                            && p.is_positive()
                        {
                            bids.push((p, q));
                        }
                        if let (Some(p), Some(q)) = (m.yes_ask_dollars, m.yes_ask_size_fp)
                            && p.is_positive()
                            && p < Fp::ONE
                        {
                            asks.push((p, q));
                        }
                        let _ = tx
                            .send(MarketEvent::BookSnapshot {
                                venue: Venue::Kalshi,
                                ticker: m.ticker.clone(),
                                ts_ms: now,
                                seq: 0,
                                bids,
                                asks,
                            })
                            .await;
                    }
                }
                for t in tickers {
                    let min_ts = last_trade_ts.get(&t).map(|ms| ms / 1000 - 1);
                    match client.get_trades(Some(&t), min_ts, None, None, 200).await {
                        Ok(r) => {
                            let mut recs = r.trades;
                            recs.sort_by_key(|x| x.created_time);
                            for rec in recs {
                                if !seen_trade_ids.insert(rec.trade_id.clone()) {
                                    continue;
                                }
                                let tr = rec.to_trade();
                                last_trade_ts.insert(t.clone(), tr.ts_ms);
                                let _ = tx.send(MarketEvent::Trade(tr)).await;
                            }
                        }
                        Err(e) => warn!(ticker = %t, error = %e, "trade poll failed"),
                    }
                }
                if seen_trade_ids.len() > 200_000 {
                    seen_trade_ids.clear();
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub async fn collect(a: CollectArgs) -> Result<()> {
    let mut feeds = start_feeds(&a.feed).await?;
    let mut rec = Recorder::new(&a.out, 60, 200_000);
    info!(out = %a.out.display(), "recording… Ctrl-C to stop");
    let mut n: u64 = 0;
    let mut last_log = std::time::Instant::now();
    loop {
        tokio::select! {
            ev = feeds.rx.recv() => {
                let Some(ev) = ev else { break };
                rec.record(&ev)?;
                n += 1;
                if last_log.elapsed().as_secs() >= 30 {
                    info!(events = n, rows_written = rec.rows_written, "collecting");
                    last_log = std::time::Instant::now();
                }
            }
            _ = tokio::signal::ctrl_c() => { info!("stopping"); break; }
        }
    }
    rec.flush()?;
    Ok(())
}

pub async fn paper(a: PaperArgs) -> Result<()> {
    let mut cfg = if a.config.exists() { Btc15mConfig::load(&a.config)? } else { Btc15mConfig::default() };
    if let Some(s) = a.feed.series.first() {
        cfg.series = s.clone();
    }
    let mut feeds = start_feeds(&a.feed).await?;
    let mut sim = SimExchange::new(SimConfig {
        mode: FillMode::Book,
        latency_ms: a.latency_ms,
        touch_ttl_ms: 2_000,
        initial_cash: Fp::from_f64(a.bankroll),
        default_fee: feeds.fee_models.get(&cfg.series).cloned().unwrap_or_else(FeeModel::kalshi_default),
        maker_touch_fill_prob: 0.5,
    });
    for (s, fm) in &feeds.fee_models {
        sim.set_fee_model(s, fm.clone());
    }
    let strat = Btc15mStrategy::new(cfg.clone());
    let mut bt = Backtester::new(sim, Box::new(strat));
    let mut rec = a.record.as_ref().map(|p| Recorder::new(p, 60, 200_000));
    info!(series = %cfg.series, bankroll = a.bankroll, authenticated = feeds.authenticated, "paper trading… Ctrl-C to stop");
    if !feeds.authenticated {
        warn!("no Kalshi API keys: using 1 Hz REST polling (touch only). Set KALSHI_API_KEY_ID / KALSHI_PRIVATE_KEY_PATH for full WebSocket books.");
    }

    let mut n_fills = 0usize;
    let mut last_log = std::time::Instant::now();
    loop {
        tokio::select! {
            ev = feeds.rx.recv() => {
                let Some(ev) = ev else { break };
                if let Some(r) = rec.as_mut() { r.record(&ev)?; }
                let before = bt.fills().len();
                bt.step(&ev);
                if bt.fills().len() > before {
                    for f in &bt.fills()[before..] {
                        info!(ticker = %f.ticker, action = ?f.action, px = %f.yes_px, qty = %f.qty, fee = %f.fee, tag = f.tag, "FILL");
                    }
                    n_fills = bt.fills().len();
                }
                if last_log.elapsed().as_secs() >= 30 {
                    let cash = bt.sim.total_cash();
                    let open: Vec<String> = bt.sim.positions().values().filter(|p| !p.yes_qty.is_zero()).map(|p| format!("{}:{}", p.ticker, p.yes_qty.fmt_dec(0))).collect();
                    let settled_pnl: f64 = bt.sim.settled.iter().map(|(_, _, _, pnl)| pnl.to_f64()).sum();
                    info!(cash = %cash, fills = n_fills, settled_pnl = format!("{settled_pnl:.2}"), ?open, "status");
                    last_log = std::time::Instant::now();
                }
            }
            _ = tokio::signal::ctrl_c() => { info!("stopping"); break; }
        }
    }
    if let Some(r) = rec.as_mut() {
        r.flush()?;
    }
    let report = bt.report(Fp::from_f64(a.bankroll));
    println!("\n{}", report.summary());
    Ok(())
}
