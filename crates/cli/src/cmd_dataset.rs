//! Build the wide historical dataset: every liquid series across every category,
//! all settled markets in the window, full candlestick price paths.
//!
//! Streams: each series is qualified, fetched and written before the next, so
//! data is usable from the first minute and `mbot universe` can re-run on a
//! growing dataset. Uses the authenticated client when keys are configured
//! (higher rate tier). Resumable: series already on disk are skipped.
//! Progress is published to `<out>/progress.json` for the dashboard.
//!
//! Layout: data/dataset/markets/<series>.parquet (DsMarket rows)
//!         data/dataset/prices/<series>.parquet  (DsPrice rows)

use anyhow::Result;
use clap::Args as ClapArgs;
use mb_data::{write_parquet, DsMarket, DsPrice};
use mb_kalshi::{KalshiClient, MarketsQuery};
use std::path::PathBuf;
use std::time::Duration;
use tracing::{info, warn};

#[derive(ClapArgs, Debug)]
pub struct Args {
    #[arg(long = "category", default_values_t = vec![
        "Crypto".to_string(), "Climate and Weather".to_string(), "Economics".to_string(), "Financials".to_string(),
        "Sports".to_string(), "Politics".to_string(), "Elections".to_string(), "Entertainment".to_string(),
        "Mentions".to_string(), "Science and Technology".to_string(), "Companies".to_string(), "World".to_string(),
        "Commodities".to_string(), "Health".to_string()])]
    pub categories: Vec<String>,
    #[arg(long, default_value_t = 45)]
    pub days: i64,
    #[arg(long, default_value_t = 200.0)]
    pub min_volume: f64,
    #[arg(long, default_value_t = 20)]
    pub min_markets: usize,
    /// Stop after this many series have price paths on disk
    #[arg(long, default_value_t = 250)]
    pub max_series: usize,
    #[arg(long, default_value_t = 120)]
    pub max_markets_per_series: usize,
    #[arg(long, default_value = "data/dataset")]
    pub out: PathBuf,
    /// ms between requests (default: 150 with API keys, 380 without)
    #[arg(long)]
    pub pace_ms: Option<u64>,
}

fn write_progress(out: &PathBuf, v: serde_json::Value) {
    let p = out.join("progress.json");
    let tmp = out.join("progress.json.tmp");
    if std::fs::write(&tmp, serde_json::to_vec(&v).unwrap_or_default()).is_ok() {
        let _ = std::fs::rename(tmp, p);
    }
}

pub async fn run(a: Args) -> Result<()> {
    let client = KalshiClient::from_env().or_else(|_| KalshiClient::public_prod())?;
    let authed = client.is_authenticated();
    let pace = Duration::from_millis(a.pace_ms.unwrap_or(if authed { 150 } else { 380 }));
    info!(authenticated = authed, pace_ms = pace.as_millis() as u64, "dataset collector");
    let now = chrono::Utc::now().timestamp();
    let since = now - a.days * 86_400;
    std::fs::create_dir_all(a.out.join("markets"))?;
    std::fs::create_dir_all(a.out.join("prices"))?;
    let started = chrono::Utc::now().timestamp_millis();

    // 1. series (all categories), largest categories first
    let mut series = Vec::new();
    for cat in &a.categories {
        match client.list_series(Some(cat)).await {
            Ok(ss) => series.extend(ss.into_iter().map(|s| (s.ticker, cat.clone(), s.title, s.frequency))),
            Err(e) => warn!(cat, error = %e, "list_series failed"),
        }
        tokio::time::sleep(pace).await;
    }
    let on_disk = std::fs::read_dir(a.out.join("prices")).map(|rd| rd.count()).unwrap_or(0);
    info!(candidates = series.len(), on_disk, "series discovered");

    // 2. stream: qualify → fetch paths → write, one series at a time
    let mut written = on_disk;
    let mut markets_fetched = 0usize;
    let mut scanned = 0usize;
    let mut qualifying = 0usize;
    for (ticker, category, title, frequency) in &series {
        if written >= a.max_series {
            break;
        }
        scanned += 1;
        if a.out.join("prices").join(format!("{ticker}.parquet")).exists() {
            continue;
        }
        let q = MarketsQuery {
            series_ticker: Some(ticker.clone()),
            status: Some("settled".into()),
            min_close_ts: Some(since),
            limit: Some(1000),
            ..Default::default()
        };
        let ms = match client.get_markets(&q, None).await {
            Ok(r) => r
                .markets
                .into_iter()
                .filter(|m| matches!(m.result.as_str(), "yes" | "no") && m.volume_fp.map(|v| v.to_f64()).unwrap_or(0.0) >= a.min_volume)
                .collect::<Vec<_>>(),
            Err(e) => {
                warn!(series = %ticker, error = %e, "markets failed");
                Vec::new()
            }
        };
        tokio::time::sleep(pace).await;
        if scanned % 25 == 0 {
            write_progress(
                &a.out,
                serde_json::json!({"phase": "collecting", "started_ms": started, "updated_ms": chrono::Utc::now().timestamp_millis(),
                                   "series_scanned": scanned, "series_candidates": series.len(), "series_qualifying": qualifying,
                                   "series_written": written, "markets_fetched": markets_fetched, "target_series": a.max_series, "authenticated": authed}),
            );
        }
        if ms.len() < a.min_markets {
            continue;
        }
        qualifying += 1;
        let mut mrows: Vec<DsMarket> = Vec::new();
        let mut prows: Vec<DsPrice> = Vec::new();
        for m in ms.iter().take(a.max_markets_per_series) {
            let (Some(open), Some(close)) = (m.open_time, m.close_time) else { continue };
            let (open_ts, close_ts) = (open.timestamp(), close.timestamp());
            let duration = (close_ts - open_ts).max(60);
            let (interval, span) = if duration <= 3 * 3600 { (1u32, duration + 120) } else { (60u32, (72 * 3600).min(duration + 3600)) };
            let candles = match client.get_candlesticks(ticker, &m.ticker, close_ts - span, close_ts, interval).await {
                Ok(c) => c,
                Err(e) => {
                    warn!(ticker = %m.ticker, error = %e, "candles failed");
                    tokio::time::sleep(pace).await;
                    continue;
                }
            };
            tokio::time::sleep(pace).await;
            markets_fetched += 1;
            mrows.push(DsMarket {
                ticker: m.ticker.clone(),
                series: ticker.clone(),
                event_ticker: m.event_ticker.clone(),
                category: category.clone(),
                frequency: frequency.clone(),
                title: m.title.clone(),
                open_ts,
                close_ts,
                result_yes: m.result == "yes",
                volume: m.volume_fp.map(|v| v.to_f64()).unwrap_or(0.0),
                strike_type: m.strike_type.clone().unwrap_or_default(),
                floor_strike: m.floor_strike,
                cap_strike: m.cap_strike,
            });
            for c in candles {
                prows.push(DsPrice {
                    ticker: m.ticker.clone(),
                    ts: c.end_period_ts,
                    secs_to_close: close_ts - c.end_period_ts,
                    bid: c.yes_bid.close_dollars.map(|p| p.to_f64()).filter(|p| *p > 0.0),
                    ask: c.yes_ask.close_dollars.map(|p| p.to_f64()).filter(|p| *p > 0.0 && *p < 1.0),
                    last: c.price.close_dollars.map(|p| p.to_f64()).filter(|p| *p > 0.0),
                    volume: c.volume_fp.map(|v| v.to_f64()).unwrap_or(0.0),
                    open_interest: c.open_interest_fp.map(|v| v.to_f64()).unwrap_or(0.0),
                });
            }
        }
        if !mrows.is_empty() {
            write_parquet(a.out.join("markets").join(format!("{ticker}.parquet")), &mrows)?;
            write_parquet(a.out.join("prices").join(format!("{ticker}.parquet")), &prows)?;
            written += 1;
            info!(series = ticker, title = %title, markets = mrows.len(), candles = prows.len(), written, "written");
            write_progress(
                &a.out,
                serde_json::json!({"phase": "collecting", "started_ms": started, "updated_ms": chrono::Utc::now().timestamp_millis(),
                                   "series_scanned": scanned, "series_candidates": series.len(), "series_qualifying": qualifying,
                                   "series_written": written, "markets_fetched": markets_fetched, "target_series": a.max_series, "authenticated": authed,
                                   "last_series": ticker, "last_title": title}),
            );
        }
    }
    write_progress(
        &a.out,
        serde_json::json!({"phase": "complete", "started_ms": started, "updated_ms": chrono::Utc::now().timestamp_millis(),
                           "series_scanned": scanned, "series_candidates": series.len(), "series_written": written, "markets_fetched": markets_fetched}),
    );
    info!(written, markets_fetched, "dataset complete");
    Ok(())
}
