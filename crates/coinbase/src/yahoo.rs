//! Yahoo Finance chart API: free 1-minute bars for futures/indices (GC=F gold,
//! SI=F silver, CL=F oil, NG=F natural gas). Used as the historical reference
//! series for Kalshi's metals/energy markets — the level differs from Kalshi's
//! settlement index (futures vs spot), which the strategy's basis estimator
//! learns from each market's strike, exactly as it does for BTC.

use crate::Candle;
use anyhow::{Context, Result};
use std::time::Duration;

/// 1-minute bars for the last `days` (Yahoo serves ~7 days of 1-minute data).
pub async fn minute_bars(symbol: &str, days: u32) -> Result<Vec<Candle>> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .user_agent("Mozilla/5.0 (compatible; marketsbot/0.1)")
        .build()?;
    let url = format!("https://query1.finance.yahoo.com/v8/finance/chart/{}?interval=1m&range={}d", urlencode(symbol), days.clamp(1, 7));
    let v: serde_json::Value = http.get(&url).send().await?.error_for_status()?.json().await.context("yahoo chart")?;
    let res = &v["chart"]["result"][0];
    let ts = res["timestamp"].as_array().context("yahoo: no timestamps")?;
    let q = &res["indicators"]["quote"][0];
    let (o, h, l, c, vol) = (&q["open"], &q["high"], &q["low"], &q["close"], &q["volume"]);
    let mut out = Vec::with_capacity(ts.len());
    for (i, t) in ts.iter().enumerate() {
        let (Some(t), Some(close)) = (t.as_i64(), c[i].as_f64()) else { continue };
        out.push(Candle {
            ts: t,
            open: o[i].as_f64().unwrap_or(close),
            high: h[i].as_f64().unwrap_or(close),
            low: l[i].as_f64().unwrap_or(close),
            close,
            volume: vol[i].as_f64().unwrap_or(0.0),
        });
    }
    out.sort_by_key(|c| c.ts);
    out.dedup_by_key(|c| c.ts);
    Ok(out)
}

fn urlencode(s: &str) -> String {
    s.replace('=', "%3D").replace('^', "%5E")
}
