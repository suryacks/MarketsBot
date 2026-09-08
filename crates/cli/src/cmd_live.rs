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
use mb_core::{Context as _, FeeModel, Fp, MarketEvent, Venue};
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
    /// Follow every open market on the exchange (touch via the ticker channel, no full books).
    /// Needed for category-scoped rules; use --category to restrict.
    #[arg(long)]
    pub all_open: bool,
    /// With --all-open: only markets whose series is in these categories (repeatable)
    #[arg(long = "category")]
    pub categories: Vec<String>,
    /// Subscribe to Kalshi's CF Benchmarks index feed (BRTI, ETHUSD_RTI, SOLUSD_RTI): the exact
    /// settlement index with its official running 60 s average. Needs API keys.
    #[arg(long)]
    pub index_feed: bool,
    /// With --all-open: only stream quotes for markets closing within this many seconds
    /// (the longest rule horizon plus slack). Keeps the subscription to a few thousand markets.
    #[arg(long, default_value_t = 30 * 3600)]
    pub horizon_max_secs: i64,
    /// With --all-open: fetch the open price (one request per market) only for these categories
    #[arg(long = "open-px-category", default_values_t = vec!["Economics".to_string()])]
    pub open_px_categories: Vec<String>,
    /// Poll NWS latest observations for the stations of the followed weather series (°F Ref events)
    #[arg(long)]
    pub nws_obs: bool,
    /// Subscribe to Kalshi's Pyth feed (Metal.Index.GOLD/USD, SILVER, XPT, XPD, PYTHOIL): the exact
    /// settlement index for the metals/energy 15-minute markets. Needs API keys.
    #[arg(long)]
    pub pyth_feed: bool,
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
    #[arg(long, default_value_t = 100.0)]
    pub bankroll: f64,
    #[arg(long, default_value_t = 250)]
    pub latency_ms: i64,
    /// Also record everything seen to this directory
    #[arg(long)]
    pub record: Option<PathBuf>,
    /// Override config: maker mode (rest post-only quotes)
    #[arg(long)]
    pub maker: bool,
    /// Override config: weight on market mid in the blended fair value
    #[arg(long)]
    pub blend: Option<f64>,
    /// Override config: realized | implied | max | mean
    #[arg(long)]
    pub vol_source: Option<String>,
    /// Override config: endgame mode (trade inside the settlement-average window)
    #[arg(long)]
    pub endgame: bool,
    /// Override config: reference symbol (e.g. BRTI with --index-feed, BTC-USD with Coinbase)
    #[arg(long)]
    pub ref_symbol: Option<String>,
    /// Override config: minimum seconds to expiry for normal-window trades (huge = endgame only)
    #[arg(long)]
    pub min_tau_secs: Option<i64>,
    /// btc15m | spread-maker | rules
    #[arg(long, default_value = "btc15m")]
    pub strategy: String,
    /// rules strategy: universe.json (PASS rows) or a rules JSON array
    #[arg(long, default_value = "reports/universe.json")]
    pub rules: PathBuf,
    /// rules strategy: which universe verdict to trade (PASS | INCONCLUSIVE)
    #[arg(long, default_value = "PASS")]
    pub rules_verdict: String,
    /// rules strategy: dollars per trade
    #[arg(long, default_value_t = 2.0)]
    pub stake: f64,
    /// Name for this run (dashboard); default derived from strategy + time
    #[arg(long)]
    pub run_id: Option<String>,
    /// Where live state snapshots go (read by `mbot dashboard`)
    #[arg(long, default_value = "data/state")]
    pub state_dir: PathBuf,
}

pub struct Feeds {
    pub rx: mpsc::Receiver<MarketEvent>,
    pub fee_models: HashMap<String, FeeModel>,
    pub authenticated: bool,
}

pub async fn start_feeds(a: &FeedArgs) -> Result<Feeds> {
    start_feeds_with(a, false).await
}

/// `with_fills`: also subscribe to the authenticated `fill` channel (live trading).
pub async fn start_feeds_with(a: &FeedArgs, with_fills: bool) -> Result<Feeds> {
    let (tx, rx) = mpsc::channel::<MarketEvent>(65_536);
    // Market data should be *production* prices even when orders go to demo. Keys are
    // environment-specific, so with demo keys the WebSocket necessarily shows demo books.
    let env_client = KalshiClient::from_env()?;
    let authenticated = env_client.is_authenticated();
    let client = if authenticated { env_client } else { KalshiClient::public_prod()? };
    info!(base = client.base(), authenticated, "kalshi market-data client");
    if authenticated && client.base().contains("demo") {
        warn!("KALSHI_ENV=demo with keys: streaming DEMO books (paper prices). Use prod keys for real market data.");
    }

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
    if a.all_open {
        // exchange-wide discovery; the touch feed subscribes explicitly to markets that can
        // still trigger a rule (closing within `horizon_max`), added dynamically as they appear
        if authenticated {
            let ws = KalshiWs::from_env()?;
            let txc = tx.clone();
            let mut channels = vec![Channel::Ticker, Channel::Trade];
            if with_fills {
                channels.push(Channel::Fill);
            }
            tokio::spawn(async move {
                if let Err(e) = ws.run_dynamic(&channels, wrx, txc).await {
                    warn!(error = %e, "kalshi ws (all-open) task ended");
                }
            });
        }
        let client = client.clone();
        let txc = tx.clone();
        let cats = a.categories.clone();
        let discover = Duration::from_secs(a.discover_secs.max(30));
        let horizon_max = a.horizon_max_secs;
        let open_px_cats = a.open_px_categories.clone();
        tokio::spawn(async move {
            if let Err(e) = discovery_all_open(client, cats, txc, wtx, discover, horizon_max, open_px_cats).await {
                warn!(error = %e, "kalshi all-open discovery ended");
            }
        });
    } else {
        if authenticated {
            let ws = KalshiWs::from_env()?;
            let txc = tx.clone();
            let mut channels = vec![Channel::OrderbookDelta, Channel::Trade, Channel::Ticker];
            if with_fills {
                channels.push(Channel::Fill);
            }
            if a.index_feed {
                channels.push(Channel::CfBenchmarks);
            }
            if a.pyth_feed {
                channels.push(Channel::PythValue);
            }
            tokio::spawn(async move {
                if let Err(e) = ws.run_dynamic(&channels, wrx, txc).await {
                    warn!(error = %e, "kalshi ws task ended");
                }
            });
        }
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

    // NWS observations for weather series
    if a.nws_obs {
        let mut stations: Vec<String> = a.series.iter().filter_map(|s| mb_coinbase::nws::station_for(s)).map(String::from).collect();
        if a.series.iter().any(|s| s == "KXRAIN") {
            stations.extend(mb_coinbase::nws::RAIN_STATIONS.iter().map(|(_, s)| s.to_string()));
        }
        stations.sort();
        stations.dedup();
        if !stations.is_empty() {
            let txc = tx.clone();
            tokio::spawn(async move {
                if let Err(e) = mb_coinbase::nws::run(stations, Duration::from_secs(60), txc).await {
                    warn!(error = %e, "nws feed ended");
                }
            });
        }
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
    let mut ignored: HashSet<String> = HashSet::new();
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
                            if ignored.contains(&info.ticker) {
                                continue;
                            }
                            // stale/zombie listings: closed long ago (strategies decide for themselves
                            // whether a market without a numeric strike is tradeable)
                            if info.close_ts_ms < chrono::Utc::now().timestamp_millis() - 60 * 60_000 {
                                warn!(ticker = %info.ticker, status = %info.status, "ignoring market closed > 1h ago");
                                ignored.insert(info.ticker.clone());
                                continue;
                            }
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
                        } else if now > info.close_ts_ms + 48 * 3_600_000 {
                            // weather/climate markets finalize hours after close; give up only after 2 days
                            warn!(ticker = %t, status = %info.status, "not settled 48 h after close; dropping");
                            settled.insert(t.clone());
                            ignored.insert(t.clone());
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

/// Exchange-wide discovery: every open market (paginated), category from the series
/// list, open price from the first hourly candle after open (only for markets that
/// opened more than an hour ago). Emits Market events for new markets and Settlement
/// for tracked markets that resolve.
async fn discovery_all_open(
    client: KalshiClient,
    categories: Vec<String>,
    tx: mpsc::Sender<MarketEvent>,
    wtx: watch::Sender<Vec<String>>,
    every: Duration,
    horizon_max_secs: i64,
    open_px_categories: Vec<String>,
) -> Result<()> {
    // series -> category
    let mut cat_of: HashMap<String, String> = HashMap::new();
    let cats: Vec<String> = if categories.is_empty() {
        vec!["Crypto", "Climate and Weather", "Economics", "Financials", "Sports", "Politics", "Elections", "Entertainment", "Mentions", "Science and Technology", "Companies", "World", "Commodities", "Health"]
            .into_iter()
            .map(String::from)
            .collect()
    } else {
        categories.clone()
    };
    for c in &cats {
        if let Ok(ss) = client.list_series(Some(c)).await {
            for s in ss {
                cat_of.insert(s.ticker, c.clone());
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    info!(series = cat_of.len(), categories = cats.len(), "all-open discovery: series→category map");
    let mut known: HashMap<String, mb_core::MarketInfo> = HashMap::new();
    let mut settled: HashSet<String> = HashSet::new();
    loop {
        let mut cursor: Option<String> = None;
        let mut seen_now: HashSet<String> = HashSet::new();
        loop {
            let q = MarketsQuery {
                status: Some("open".into()),
                limit: Some(1000),
                ..Default::default()
            };
            let r = match client.get_markets(&q, cursor.as_deref()).await {
                Ok(r) => r,
                Err(e) => {
                    warn!(error = %e, "all-open markets page failed");
                    break;
                }
            };
            let n = r.markets.len();
            for m in r.markets {
                let mut info = m.to_info();
                let Some(cat) = cat_of.get(&info.series) else { continue };
                if !categories.is_empty() && !categories.contains(cat) {
                    continue;
                }
                info.category = cat.clone();
                seen_now.insert(info.ticker.clone());
                if known.contains_key(&info.ticker) {
                    continue;
                }
                // open price: first hourly candle after open (one request per market — only where drift rules apply)
                let now_s = chrono::Utc::now().timestamp();
                if open_px_categories.contains(cat) && info.open_ts_ms > 0 && now_s - info.open_ts_ms / 1000 > 3600 {
                    let o = info.open_ts_ms / 1000;
                    if let Ok(cs) = client.get_candlesticks(&info.series, &info.ticker, o, o + 6 * 3600, 60).await {
                        if let Some(c) = cs.iter().find(|c| c.price.close_dollars.is_some() || c.yes_bid.close_dollars.is_some()) {
                            info.open_px = match (c.yes_bid.close_dollars, c.yes_ask.close_dollars) {
                                (Some(b), Some(a)) if b.is_positive() && a < Fp::ONE => Some(Fp((b.0 + a.0) / 2)),
                                _ => c.price.close_dollars,
                            };
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(120)).await;
                }
                known.insert(info.ticker.clone(), info.clone());
                let _ = tx.send(MarketEvent::Market(info)).await;
            }
            cursor = if r.cursor.is_empty() || n == 0 { None } else { Some(r.cursor) };
            if cursor.is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        // stream quotes only for markets that can still trigger a rule
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut soon: Vec<String> = known
            .iter()
            .filter(|(_, m)| m.close_ts_ms > now_ms && m.close_ts_ms - now_ms <= horizon_max_secs * 1000)
            .map(|(t, _)| t.clone())
            .collect();
        soon.sort();
        let _ = wtx.send(soon.clone());
        info!(open_markets = seen_now.len(), tracked = known.len(), streaming = soon.len(), "all-open discovery pass");
        // settlement checks for tracked markets past close (kept for up to 48 h — weather
        // buckets finalize hours after close)
        let now = chrono::Utc::now().timestamp_millis();
        known.retain(|_, m| !(m.close_ts_ms > 0 && now > m.close_ts_ms + 48 * 3_600_000));
        let past: Vec<String> = known
            .iter()
            .filter(|(t, m)| m.close_ts_ms > 0 && now > m.close_ts_ms + 60_000 && !settled.contains(*t))
            .map(|(t, _)| t.clone())
            .take(50)
            .collect();
        for t in past {
            if let Ok(m) = client.get_market(&t).await {
                let info = m.to_info();
                if let Some(r) = info.settled_outcome() {
                    settled.insert(t.clone());
                    let _ = tx
                        .send(MarketEvent::Settlement {
                            venue: Venue::Kalshi,
                            ticker: t.clone(),
                            ts_ms: now,
                            result: r,
                        })
                        .await;
                    known.remove(&t);
                }
            }
            tokio::time::sleep(Duration::from_millis(120)).await;
        }
        tokio::time::sleep(every).await;
    }
}

#[derive(ClapArgs, Debug)]
pub struct LiveArgs {
    #[command(flatten)]
    pub paper: PaperArgs,
    /// Hard cap on worst-case dollars at risk (positions + resting orders)
    #[arg(long, default_value_t = 100.0)]
    pub max_notional: f64,
    #[arg(long, default_value_t = 5.0)]
    pub max_order_qty: f64,
    #[arg(long, default_value_t = 20)]
    pub max_open_orders: usize,
    /// Kill switch: halt and cancel everything once equity drops this much below start
    #[arg(long, default_value_t = 50.0)]
    pub max_loss: f64,
    /// Log orders instead of sending them
    #[arg(long)]
    pub dry_run: bool,
    /// Required to send real orders against the production exchange
    #[arg(long)]
    pub i_understand_this_uses_real_money: bool,
}

pub async fn live(a: LiveArgs) -> Result<()> {
    let (rest, _) = mb_kalshi::env_urls();
    let is_prod = rest.contains("external-api.kalshi.com");
    if is_prod && !a.dry_run && !a.i_understand_this_uses_real_money {
        anyhow::bail!("KALSHI_ENV=prod: pass --i-understand-this-uses-real-money (or --dry-run, or set KALSHI_ENV=demo)");
    }
    let client = KalshiClient::from_env()?;
    if !client.is_authenticated() {
        anyhow::bail!("live trading needs KALSHI_API_KEY_ID / KALSHI_PRIVATE_KEY_PATH");
    }
    let (mut strat, series_label) = build_strategy(&a.paper)?;
    let run_id = a
        .paper
        .run_id
        .clone()
        .unwrap_or_else(|| format!("LIVE-{}-{}", strat.name(), chrono::Utc::now().format("%Y%m%d-%H%M")));
    let started_ms = chrono::Utc::now().timestamp_millis();
    let state_path = a.paper.state_dir.join(format!("{run_id}.json"));
    let mut feeds = start_feeds_with(&a.paper.feed, true).await?;
    let cfg = mb_live::LiveConfig {
        max_notional: a.max_notional,
        max_order_qty: a.max_order_qty,
        max_open_orders: a.max_open_orders,
        max_loss: a.max_loss,
        dry_run: a.dry_run,
    };
    let mut ex = mb_live::KalshiExecutor::new(client, cfg).await?;
    for (s, fm) in &feeds.fee_models {
        ex.set_fee_model(s, fm.clone());
    }
    let mut rec = a.paper.record.as_ref().map(|p| Recorder::new(p, 60, 200_000));
    warn!(run_id, series = %series_label, env = if is_prod { "PROD" } else { "demo" }, dry_run = a.dry_run,
          max_notional = a.max_notional, max_loss = a.max_loss, "LIVE TRADING — Ctrl-C to stop (resting orders are cancelled on exit)");

    let mut state_tick = tokio::time::interval(Duration::from_secs(1));
    let mut sync_tick = tokio::time::interval(Duration::from_secs(60));
    sync_tick.tick().await;
    loop {
        tokio::select! {
            _ = state_tick.tick() => {
                let st = ex.state(&run_id, started_ms, strat.name(), strat.snapshot());
                if let Err(e) = Backtester::write_state(&state_path, &st) { warn!(error = %e, "state write failed"); }
            }
            _ = sync_tick.tick() => {
                if let Err(e) = ex.sync_account().await { warn!(error = %e, "account resync failed"); }
            }
            ev = feeds.rx.recv() => {
                let Some(ev) = ev else { break };
                if let Some(r) = rec.as_mut() { r.record(&ev)?; }
                ex.on_event(&ev);
                strat.on_event(&ev, &mut ex);
                for f in ex.drain_fills() {
                    strat.on_fill(&f, &mut ex);
                }
            }
            _ = tokio::signal::ctrl_c() => { info!("stopping: cancelling resting orders"); break; }
        }
    }
    let ids: Vec<mb_core::OrderId> = ex.open_orders_all();
    for id in ids {
        ex.cancel(id);
    }
    tokio::time::sleep(Duration::from_secs(2)).await; // let the worker flush cancels
    if let Some(r) = rec.as_mut() {
        r.flush()?;
    }
    let mut st = ex.state(&run_id, started_ms, strat.name(), strat.snapshot());
    st["stopped_ms"] = serde_json::json!(chrono::Utc::now().timestamp_millis());
    let _ = Backtester::write_state(&state_path, &st);
    println!("{}", serde_json::to_string_pretty(&st["risk"])?);
    Ok(())
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

/// Build the strategy named by `--strategy`, applying CLI overrides.
pub fn build_strategy(a: &PaperArgs) -> Result<(Box<dyn mb_core::Strategy>, String)> {
    match a.strategy.as_str() {
        "rules" => {
            let cfg = mb_strategy::RuleTraderConfig::from_json(&a.rules, &a.rules_verdict, 0.0, a.stake)?;
            if cfg.rules.is_empty() {
                anyhow::bail!("no {} rules in {}", a.rules_verdict, a.rules.display());
            }
            info!(rules = cfg.rules.len(), stake = cfg.stake, "rule trader");
            let series = format!("{} rules", cfg.rules.len());
            Ok((Box::new(mb_strategy::RuleTrader::new(cfg)), series))
        }
        "weather-lock" | "weather_lock" => {
            let mut cfg = mb_strategy::WeatherLockConfig::default();
            cfg.stake = a.stake;
            let series = cfg.stations.keys().cloned().collect::<Vec<_>>().join(",");
            Ok((Box::new(mb_strategy::WeatherLock::new(cfg)), series))
        }
        "spread-maker" | "spread_maker" => {
            let path = if a.config.to_string_lossy().contains("btc15m") { PathBuf::from("strategies/spread_maker.toml") } else { a.config.clone() };
            let mut cfg = if path.exists() { mb_strategy::SpreadMakerConfig::load(&path)? } else { mb_strategy::SpreadMakerConfig::default() };
            if !a.feed.series.is_empty() && a.feed.series != vec!["KXBTC15M".to_string()] {
                cfg.series = a.feed.series.clone();
            }
            cfg.scale_to_bankroll(a.bankroll);
            let series = cfg.series.join(",");
            Ok((Box::new(mb_strategy::SpreadMaker::new(cfg)), series))
        }
        _ => {
            let mut cfg = if a.config.exists() { Btc15mConfig::load(&a.config)? } else { Btc15mConfig::default() };
            if let Some(s) = a.feed.series.first() {
                cfg.series = s.clone();
            }
            if a.maker {
                cfg.maker = true;
            }
            if let Some(b) = a.blend {
                cfg.market_blend = b;
            }
            if let Some(v) = &a.vol_source {
                cfg.vol_source = v.clone();
            }
            if a.endgame {
                cfg.endgame = true;
                cfg.max_entries_per_market = cfg.max_entries_per_market.max(6);
            }
            if let Some(r) = &a.ref_symbol {
                cfg.ref_symbol = r.clone();
            }
            if let Some(t) = a.min_tau_secs {
                cfg.min_tau_secs = t;
            }
            cfg.scale_to_bankroll(a.bankroll);
            let series = cfg.series.clone();
            Ok((Box::new(Btc15mStrategy::new(cfg)), series))
        }
    }
}

pub async fn paper(a: PaperArgs) -> Result<()> {
    let (strat, series_label) = build_strategy(&a)?;
    let run_id = a
        .run_id
        .clone()
        .unwrap_or_else(|| format!("paper-{}-{}", strat.name(), chrono::Utc::now().format("%Y%m%d-%H%M")));
    let started_ms = chrono::Utc::now().timestamp_millis();
    let state_path = a.state_dir.join(format!("{run_id}.json"));
    let mut feeds = start_feeds(&a.feed).await?;
    let default_fee = feeds.fee_models.values().next().cloned().unwrap_or_else(FeeModel::kalshi_default);
    let mut sim = SimExchange::new(SimConfig {
        mode: FillMode::Book,
        latency_ms: a.latency_ms,
        touch_ttl_ms: 2_000,
        initial_cash: Fp::from_f64(a.bankroll),
        default_fee,
        maker_touch_fill_prob: 0.5,
    });
    for (s, fm) in &feeds.fee_models {
        sim.set_fee_model(s, fm.clone());
    }
    let mut bt = Backtester::new(sim, strat);
    let mut rec = a.record.as_ref().map(|p| Recorder::new(p, 60, 200_000));
    info!(run_id, series = %series_label, bankroll = a.bankroll, authenticated = feeds.authenticated, state = %state_path.display(), "paper trading… Ctrl-C to stop");
    if !feeds.authenticated {
        warn!("no Kalshi API keys: using 1 Hz REST polling (touch only). Set KALSHI_API_KEY_ID / KALSHI_PRIVATE_KEY_PATH for full WebSocket books.");
    }

    let mut n_fills = 0usize;
    let mut last_log = std::time::Instant::now();
    let mut state_tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = state_tick.tick() => {
                let st = bt.state(&run_id, "paper", Fp::from_f64(a.bankroll), started_ms);
                if let Err(e) = Backtester::write_state(&state_path, &st) { warn!(error = %e, "state write failed"); }
            }
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
                    let qs = &bt.sim.queue_stats;
                    info!(cash = %cash, fills = n_fills, settled_pnl = format!("{settled_pnl:.2}"), ?open,
                          rested = qs.orders_rested, avg_ahead = format!("{:.0}", if qs.orders_rested > 0 { qs.ahead_at_insert / qs.orders_rested as f64 } else { 0.0 }),
                          front = qs.reached_front, at_px = qs.fills_at_price, through = qs.fills_through, "status");
                    last_log = std::time::Instant::now();
                }
            }
            _ = tokio::signal::ctrl_c() => { info!("stopping"); break; }
        }
    }
    if let Some(r) = rec.as_mut() {
        r.flush()?;
    }
    let mut st = bt.state(&run_id, "paper", Fp::from_f64(a.bankroll), started_ms);
    st["stopped_ms"] = serde_json::json!(chrono::Utc::now().timestamp_millis());
    let _ = Backtester::write_state(&state_path, &st);
    let report = bt.report(Fp::from_f64(a.bankroll));
    println!("\n{}", report.summary());
    Ok(())
}
