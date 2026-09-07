use anyhow::Result;
use clap::Args as ClapArgs;
use futures_util::{stream, StreamExt};
use mb_coinbase::CoinbaseRest;
use mb_data::{write_parquet, CandleRow, MarketRow, RefRow, TradeRow};
use mb_kalshi::{KalshiClient, MarketsQuery};
use std::path::PathBuf;
use tracing::{info, warn};

#[derive(ClapArgs, Debug)]
pub struct Args {
    /// Kalshi series ticker, e.g. KXBTC15M, KXETH15M
    #[arg(long, default_value = "KXBTC15M")]
    pub series: String,
    /// How many days back to fetch settled markets
    #[arg(long, default_value_t = 7)]
    pub days: i64,
    /// Data root directory
    #[arg(long, default_value = "data")]
    pub out: PathBuf,
    /// Coinbase product for reference candles (empty to skip)
    #[arg(long, default_value = "BTC-USD")]
    pub ref_product: String,
    /// Parallel trade-tape downloads
    #[arg(long, default_value_t = 6)]
    pub concurrency: usize,
    /// Re-download trade files that already exist
    #[arg(long)]
    pub force: bool,
    /// Also pull tick-level Coinbase trades (1-second last price) into data/refs — slower (~10 req/s) but
    /// removes the 60 s staleness of candles.
    #[arg(long)]
    pub ref_ticks: bool,
    /// Skip Kalshi markets/trades (only refresh reference data)
    #[arg(long)]
    pub refs_only: bool,
}

pub async fn run(a: Args) -> Result<()> {
    let client = KalshiClient::public_prod()?;
    let now = chrono::Utc::now().timestamp();
    let from_ts = now - a.days * 86_400;

    // 1. settled markets
    info!(series = %a.series, days = a.days, "fetching settled markets");
    let markets = client
        .get_all_markets(&MarketsQuery {
            series_ticker: Some(a.series.clone()),
            status: Some("settled".into()),
            min_close_ts: Some(from_ts),
            ..Default::default()
        })
        .await?;
    let mut rows: Vec<MarketRow> = markets.iter().map(|m| MarketRow::from(&m.to_info())).collect();
    rows.sort_by_key(|r| r.close_ts_ms);
    let (min_open, max_close) = rows
        .iter()
        .fold((i64::MAX, 0i64), |(lo, hi), r| (lo.min(r.open_ts_ms), hi.max(r.close_ts_ms)));
    write_parquet(a.out.join("markets").join(format!("{}.parquet", a.series)), &rows)?;
    info!(n = rows.len(), "wrote markets");
    if rows.is_empty() {
        return Ok(());
    }

    // 2. trade tape per market (idempotent: one file per ticker)
    let trades_dir = a.out.join("trades").join(&a.series);
    std::fs::create_dir_all(&trades_dir)?;
    let todo: Vec<String> = if a.refs_only {
        Vec::new()
    } else {
        rows.iter()
            .map(|r| r.ticker.clone())
            .filter(|t| a.force || !trades_dir.join(format!("{t}.parquet")).exists())
            .collect()
    };
    info!(total = rows.len(), to_fetch = todo.len(), "fetching trade tapes");
    let total_trades = std::sync::atomic::AtomicU64::new(0);
    let done = std::sync::atomic::AtomicU64::new(0);
    stream::iter(todo)
        .map(|ticker| {
            let client = client.clone();
            let dir = trades_dir.clone();
            let total_trades = &total_trades;
            let done = &done;
            let n_todo = rows.len();
            async move {
                match client.get_all_trades(&ticker).await {
                    Ok(recs) => {
                        let mut rows: Vec<TradeRow> = recs.iter().map(|r| TradeRow::from(&r.to_trade())).collect();
                        rows.sort_by_key(|r| r.ts_ms);
                        if let Err(e) = write_parquet(dir.join(format!("{ticker}.parquet")), &rows) {
                            warn!(ticker, error = %e, "write failed");
                        }
                        let t = total_trades.fetch_add(rows.len() as u64, std::sync::atomic::Ordering::Relaxed) + rows.len() as u64;
                        let d = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                        if d % 25 == 0 {
                            info!(done = d, of = n_todo, trades = t, "progress");
                        }
                    }
                    Err(e) => warn!(ticker, error = %e, "trade fetch failed"),
                }
            }
        })
        .buffer_unordered(a.concurrency)
        .collect::<Vec<()>>()
        .await;
    info!(trades = total_trades.load(std::sync::atomic::Ordering::Relaxed), "trade tapes done");

    // 3. reference candles (1-minute), per UTC day, with a 2h warmup for vol seeding
    if !a.ref_product.is_empty() {
        let cb = CoinbaseRest::new()?;
        let start = min_open / 1000 - 7_200;
        let end = (max_close / 1000 + 120).min(now);
        let mut day = start - start.rem_euclid(86_400);
        while day < end {
            let day_str = chrono::DateTime::from_timestamp(day, 0).unwrap().format("%Y-%m-%d").to_string();
            let path = a.out.join("candles").join(&a.ref_product).join(format!("{day_str}.parquet"));
            let is_today = day + 86_400 > now;
            if path.exists() && !a.force && !is_today {
                day += 86_400;
                continue;
            }
            let s = day.max(start);
            let e = (day + 86_400).min(end);
            let candles = cb.candles_range(&a.ref_product, 60, s, e).await?;
            let rows: Vec<CandleRow> = candles
                .iter()
                .map(|c| CandleRow {
                    source: mb_coinbase::SOURCE.into(),
                    symbol: a.ref_product.clone(),
                    ts: c.ts,
                    open: c.open,
                    high: c.high,
                    low: c.low,
                    close: c.close,
                    volume: c.volume,
                })
                .collect();
            write_parquet(&path, &rows)?;
            info!(day = %day_str, candles = rows.len(), "wrote candles");
            day += 86_400;
        }

        // 4. tick-level reference prices, downsampled to the last trade of each second.
        //    Coinbase only pages backwards from the newest trade, so fetch the whole
        //    missing span in ONE pass (oldest missing day → end) and split by UTC day.
        if a.ref_ticks {
            let refs_dir = a.out.join("refs").join(&a.ref_product);
            let day_path = |day: i64| {
                let d = chrono::DateTime::from_timestamp(day, 0).unwrap().format("%Y-%m-%d").to_string();
                refs_dir.join(format!("{d}.parquet"))
            };
            let mut missing: Vec<i64> = Vec::new();
            let mut day = start - start.rem_euclid(86_400);
            while day < end {
                let is_today = day + 86_400 > now;
                if a.force || is_today || !day_path(day).exists() {
                    missing.push(day);
                }
                day += 86_400;
            }
            if let Some(&first) = missing.first() {
                let s_ms = first.max(start) * 1000;
                let e_ms = end * 1000;
                info!(days = missing.len(), from = %chrono::DateTime::from_timestamp(first, 0).unwrap().date_naive(), "fetching coinbase ticks in one pass (~40 s per day)");
                let ticks = cb
                    .trades_range(&a.ref_product, s_ms, e_ms, |n, oldest| {
                        info!(ticks = n, oldest = %chrono::DateTime::from_timestamp_millis(oldest).unwrap().to_rfc3339(), "…");
                    })
                    .await?;
                for &day in &missing {
                    let lo = day * 1000;
                    let hi = (day + 86_400) * 1000;
                    let mut rows: Vec<RefRow> = Vec::new();
                    let mut n_ticks = 0usize;
                    for t in ticks.iter().filter(|t| t.ts_ms >= lo && t.ts_ms < hi) {
                        n_ticks += 1;
                        let sec = t.ts_ms.div_euclid(1000);
                        match rows.last_mut() {
                            Some(last) if last.ts_ms.div_euclid(1000) == sec => {
                                last.ts_ms = t.ts_ms;
                                last.px = t.px;
                            }
                            _ => rows.push(RefRow {
                                source: mb_coinbase::SOURCE.into(),
                                symbol: a.ref_product.clone(),
                                ts_ms: t.ts_ms,
                                px: t.px,
                            }),
                        }
                    }
                    if rows.is_empty() {
                        continue;
                    }
                    let p = day_path(day);
                    write_parquet(&p, &rows)?;
                    info!(path = %p.display(), ticks = n_ticks, seconds = rows.len(), "wrote refs");
                }
            } else {
                info!("tick refs already present for the whole range");
            }
        }
    }
    info!("done. next: mbot backtest --series {} --data {}", a.series, a.out.display());
    Ok(())
}
