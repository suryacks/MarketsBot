//! Exchange-wide calibration / bias scan.
//!
//! For every settled market in the chosen series: sample the YES price at fixed
//! horizons before close (from public candlesticks), bucket by price, and measure
//! the realized YES rate. Where realized ≠ price by more than fees, and the
//! sample is large, there is a systematic mispricing worth building around.
//! Output: ranked table + `reports/bias-scan-<tag>.json` (read by the dashboard).

use anyhow::Result;
use clap::Args as ClapArgs;
use mb_core::{FeeModel, Fp};
use mb_kalshi::{KalshiClient, MarketsQuery};
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tracing::{info, warn};

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// Series tickers (repeatable). If empty, discover from --category.
    #[arg(long = "series")]
    pub series: Vec<String>,
    /// Kalshi categories to discover series from (repeatable)
    #[arg(long = "category", default_values_t = vec!["Crypto".to_string(), "Climate and Weather".to_string(), "Economics".to_string(), "Financials".to_string(), "Sports".to_string(), "Politics".to_string()])]
    pub categories: Vec<String>,
    #[arg(long, default_value_t = 30)]
    pub days: i64,
    /// Skip markets with less than this many contracts traded
    #[arg(long, default_value_t = 500.0)]
    pub min_volume: f64,
    /// Max markets to sample per series (most recent first)
    #[arg(long, default_value_t = 400)]
    pub max_markets_per_series: usize,
    /// Only series with at least this many qualifying settled markets
    #[arg(long, default_value_t = 30)]
    pub min_markets: usize,
    /// Max number of series to scan (largest by settled-market count first)
    #[arg(long, default_value_t = 60)]
    pub max_series: usize,
    #[arg(long, default_value = "reports")]
    pub out: PathBuf,
    #[arg(long, default_value = "scan")]
    pub tag: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BucketRow {
    pub series: String,
    pub category: String,
    pub horizon_secs: i64,
    pub bucket_lo: f64,
    pub bucket_hi: f64,
    pub n: usize,
    pub avg_px: f64,
    pub yes_rate: f64,
    /// Expected PnL per contract of buying YES at avg_px after taker fee.
    pub ev_buy_yes: f64,
    /// Expected PnL per contract of buying NO (= selling YES) after taker fee.
    pub ev_buy_no: f64,
    /// |best EV| / standard error of the realized rate.
    pub t_stat: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SeriesSummary {
    pub series: String,
    pub category: String,
    pub title: String,
    pub markets: usize,
    pub samples: usize,
    pub brier_market: f64,
}

struct Sample {
    series: String,
    horizon: i64,
    px: f64,
    yes: bool,
}

fn horizons_for(duration_secs: i64) -> (u32, Vec<i64>) {
    if duration_secs <= 3 * 3600 {
        (1, vec![120, 300, 600, 900, 1800].into_iter().filter(|h| *h < duration_secs).collect())
    } else if duration_secs <= 3 * 86_400 {
        (60, vec![3600, 3 * 3600, 6 * 3600, 12 * 3600, 24 * 3600].into_iter().filter(|h| *h < duration_secs).collect())
    } else {
        (60, vec![3600, 6 * 3600, 24 * 3600, 48 * 3600].into_iter().filter(|h| *h < duration_secs).collect())
    }
}

pub async fn run(a: Args) -> Result<()> {
    let client = KalshiClient::public_prod()?;
    let now = chrono::Utc::now().timestamp();
    let since = now - a.days * 86_400;
    let pace = Duration::from_millis(350);

    // 1. series discovery
    let mut series: Vec<(String, String, String)> = Vec::new(); // (ticker, category, title)
    if a.series.is_empty() {
        for cat in &a.categories {
            match client.list_series(Some(cat)).await {
                Ok(ss) => {
                    for s in ss {
                        if matches!(s.frequency.as_str(), "daily" | "hourly" | "fifteen_min" | "weekly" | "custom" | "monthly") {
                            series.push((s.ticker, cat.clone(), s.title));
                        }
                    }
                }
                Err(e) => warn!(cat, error = %e, "list_series failed"),
            }
            tokio::time::sleep(pace).await;
        }
    } else {
        for s in &a.series {
            let sr = client.get_series(s).await.ok();
            series.push((s.clone(), sr.as_ref().map(|x| x.category.clone()).unwrap_or_default(), sr.map(|x| x.title).unwrap_or_default()));
        }
    }
    info!(candidates = series.len(), "series discovered");

    // 2. settled markets per series (first page only: 1000 most recent)
    let mut chosen: Vec<((String, String, String), Vec<mb_kalshi::types::Market>)> = Vec::new();
    for (i, s) in series.iter().enumerate() {
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
        if i % 25 == 0 {
            info!(scanned = i + 1, of = series.len(), qualifying = chosen.len(), "series pass");
        }
        tokio::time::sleep(pace).await;
    }
    chosen.sort_by_key(|(_, ms)| std::cmp::Reverse(ms.len()));
    chosen.truncate(a.max_series);
    let total_markets: usize = chosen.iter().map(|(_, ms)| ms.len().min(a.max_markets_per_series)).sum();
    info!(series = chosen.len(), markets = total_markets, eta_min = total_markets as f64 * 0.36 / 60.0, "sampling candlesticks");

    // 3. sample prices at horizons before close
    let fee = FeeModel::kalshi_default();
    let mut samples: Vec<Sample> = Vec::new();
    let mut summaries: Vec<SeriesSummary> = Vec::new();
    let mut done = 0usize;
    for ((ticker, category, title), ms) in &chosen {
        let mut n_samples = 0usize;
        let mut brier = 0.0;
        for m in ms.iter().take(a.max_markets_per_series) {
            let (Some(open), Some(close)) = (m.open_time, m.close_time) else { continue };
            let close_ts = close.timestamp();
            let duration = (close_ts - open.timestamp()).max(60);
            let (interval, horizons) = horizons_for(duration);
            if horizons.is_empty() {
                continue;
            }
            let span = horizons.iter().max().copied().unwrap_or(3600) + interval as i64 * 60 * 2;
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
            if done % 100 == 0 {
                info!(done, of = total_markets, samples = samples.len(), "progress");
            }
            let yes = m.result == "yes";
            for h in &horizons {
                let t = close_ts - h;
                // last candle ending at or before t
                let Some(c) = candles.iter().filter(|c| c.end_period_ts <= t).max_by_key(|c| c.end_period_ts) else { continue };
                if t - c.end_period_ts > interval as i64 * 60 * 3 {
                    continue; // stale
                }
                let px = match (c.yes_bid.close_dollars, c.yes_ask.close_dollars) {
                    (Some(b), Some(a)) if b.is_positive() && a < Fp::ONE => (b.to_f64() + a.to_f64()) / 2.0,
                    _ => match c.price.close_dollars {
                        Some(p) if p.is_positive() => p.to_f64(),
                        _ => continue,
                    },
                };
                if !(0.005..=0.995).contains(&px) {
                    continue;
                }
                samples.push(Sample {
                    series: ticker.clone(),
                    horizon: *h,
                    px,
                    yes,
                });
                n_samples += 1;
                brier += (px - if yes { 1.0 } else { 0.0 }).powi(2);
            }
        }
        summaries.push(SeriesSummary {
            series: ticker.clone(),
            category: category.clone(),
            title: title.clone(),
            markets: ms.len().min(a.max_markets_per_series),
            samples: n_samples,
            brier_market: if n_samples > 0 { brier / n_samples as f64 } else { f64::NAN },
        });
    }

    // 4. bucket
    let cat_of: HashMap<&str, &str> = chosen.iter().map(|((t, c, _), _)| (t.as_str(), c.as_str())).collect();
    let mut buckets: HashMap<(String, i64, i32), Vec<&Sample>> = HashMap::new();
    for s in &samples {
        let b = ((s.px * 20.0).floor() as i32).clamp(0, 19); // 5¢ buckets
        buckets.entry((s.series.clone(), s.horizon, b)).or_default().push(s);
        buckets.entry(("ALL".into(), s.horizon, b)).or_default().push(s);
        buckets.entry((format!("CAT:{}", cat_of.get(s.series.as_str()).copied().unwrap_or("?")), s.horizon, b)).or_default().push(s);
    }
    let mut rows: Vec<BucketRow> = buckets
        .into_iter()
        .filter(|(_, v)| v.len() >= 20)
        .map(|((series, horizon, b), v)| {
            let n = v.len();
            let avg_px = v.iter().map(|s| s.px).sum::<f64>() / n as f64;
            let yes_rate = v.iter().filter(|s| s.yes).count() as f64 / n as f64;
            let p = Fp::from_f64(avg_px);
            let ev_buy_yes = yes_rate - avg_px - fee.fee_per_contract(p, false);
            let ev_buy_no = avg_px - yes_rate - fee.fee_per_contract(p, false);
            let se = (yes_rate * (1.0 - yes_rate) / n as f64).sqrt().max(1e-6);
            let best = ev_buy_yes.max(ev_buy_no);
            BucketRow {
                category: if series.starts_with("CAT:") || series == "ALL" { series.clone() } else { cat_of.get(series.as_str()).copied().unwrap_or("").to_string() },
                series,
                horizon_secs: horizon,
                bucket_lo: b as f64 / 20.0,
                bucket_hi: (b + 1) as f64 / 20.0,
                n,
                avg_px,
                yes_rate,
                ev_buy_yes,
                ev_buy_no,
                t_stat: best / se,
            }
        })
        .collect();
    rows.sort_by(|x, y| y.t_stat.partial_cmp(&x.t_stat).unwrap_or(std::cmp::Ordering::Equal));

    println!("\n== systematic mispricings (5¢ buckets, taker fees included; t = edge / s.e.) ==");
    println!("{:<22} {:>7} {:>9} {:>5} {:>6} {:>6} {:>8} {:>8} {:>6}", "series", "horizon", "bucket", "n", "px", "yes%", "EV yes", "EV no", "t");
    for r in rows.iter().filter(|r| r.n >= 30 && r.t_stat > 2.0).take(40) {
        println!(
            "{:<22} {:>6}s {:>4.2}-{:<4.2} {:>5} {:>6.3} {:>6.3} {:>+8.3} {:>+8.3} {:>6.2}",
            r.series, r.horizon_secs, r.bucket_lo, r.bucket_hi, r.n, r.avg_px, r.yes_rate, r.ev_buy_yes, r.ev_buy_no, r.t_stat
        );
    }
    println!("\n== per-series market Brier (lower = better-priced market) ==");
    summaries.sort_by(|x, y| x.brier_market.partial_cmp(&y.brier_market).unwrap_or(std::cmp::Ordering::Equal));
    for s in &summaries {
        println!("{:<20} {:<22} {:<40} markets {:>4} samples {:>5} brier {:.4}", s.series, s.category, s.title.chars().take(40).collect::<String>(), s.markets, s.samples, s.brier_market);
    }

    std::fs::create_dir_all(&a.out)?;
    let path = a.out.join(format!("bias-{}.json", a.tag));
    let out = serde_json::json!({
        "kind": "bias-scan", "created_ms": chrono::Utc::now().timestamp_millis(), "days": a.days,
        "min_volume": a.min_volume, "series": summaries, "rows": rows, "n_samples": samples.len(),
    });
    std::fs::write(&path, serde_json::to_vec_pretty(&out)?)?;
    info!(path = %path.display(), rows = out["rows"].as_array().map(|r| r.len()).unwrap_or(0), "scan written");
    Ok(())
}
