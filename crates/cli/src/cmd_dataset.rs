//! Build the wide historical dataset: every liquid series across every category,
//! all settled markets in the window, full candlestick price paths. Public
//! endpoints only, paced. Resumable: series already on disk are skipped.
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
    #[arg(long, default_value_t = 200)]
    pub max_series: usize,
    #[arg(long, default_value_t = 120)]
    pub max_markets_per_series: usize,
    #[arg(long, default_value = "data/dataset")]
    pub out: PathBuf,
    /// ms between requests
    #[arg(long, default_value_t = 380)]
    pub pace_ms: u64,
}

pub async fn run(a: Args) -> Result<()> {
    let client = KalshiClient::public_prod()?;
    let pace = Duration::from_millis(a.pace_ms);
    let now = chrono::Utc::now().timestamp();
    let since = now - a.days * 86_400;
    std::fs::create_dir_all(a.out.join("markets"))?;
    std::fs::create_dir_all(a.out.join("prices"))?;

    // 1. series
    let mut series = Vec::new();
    for cat in &a.categories {
        match client.list_series(Some(cat)).await {
            Ok(ss) => series.extend(ss.into_iter().map(|s| (s.ticker, cat.clone(), s.title, s.frequency))),
            Err(e) => warn!(cat, error = %e, "list_series failed"),
        }
        tokio::time::sleep(pace).await;
    }
    info!(candidates = series.len(), "series discovered");

    // 2. settled markets per series (skip series already on disk)
    let mut chosen: Vec<((String, String, String, String), Vec<mb_kalshi::types::Market>)> = Vec::new();
    for (i, s) in series.iter().enumerate() {
        if a.out.join("prices").join(format!("{}.parquet", s.0)).exists() {
            continue;
        }
        let q = MarketsQuery {
            series_ticker: Some(s.0.clone()),
            status: Some("settled".into()),
            min_close_ts: Some(since),
            limit: Some(1000),
            ..Default::default()
        };
        match client.get_markets(&q, None).await {
            Ok(r) => {
                let ms: Vec<_> = r
                    .markets
                    .into_iter()
                    .filter(|m| matches!(m.result.as_str(), "yes" | "no") && m.volume_fp.map(|v| v.to_f64()).unwrap_or(0.0) >= a.min_volume)
                    .collect();
                if ms.len() >= a.min_markets {
                    chosen.push((s.clone(), ms));
                }
            }
            Err(e) => warn!(series = %s.0, error = %e, "markets failed"),
        }
        if i % 100 == 0 {
            info!(scanned = i + 1, of = series.len(), qualifying = chosen.len(), "series pass");
        }
        tokio::time::sleep(pace).await;
    }
    chosen.sort_by_key(|(_, ms)| std::cmp::Reverse(ms.len()));
    chosen.truncate(a.max_series);
    let total: usize = chosen.iter().map(|(_, ms)| ms.len().min(a.max_markets_per_series)).sum();
    info!(series = chosen.len(), markets = total, eta_h = total as f64 * a.pace_ms as f64 / 3.6e6, "fetching price paths");

    // 3. candles per market, written per series
    let mut done = 0usize;
    for ((ticker, category, title, frequency), ms) in &chosen {
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
            done += 1;
            if done % 200 == 0 {
                info!(done, of = total, "progress");
            }
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
            info!(series = ticker, title = %title, markets = mrows.len(), candles = prows.len(), "written");
        }
    }
    info!("dataset complete");
    Ok(())
}
