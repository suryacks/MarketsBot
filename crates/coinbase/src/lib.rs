//! Coinbase Exchange public data: historical 1-minute candles (for backtests)
//! and the live `ticker` WebSocket (reference price for crypto strategies).
//! Also hosts the NWS observation feed (`nws` module) — external reference data.
//!
//! Kalshi's crypto markets settle on CF Benchmarks' BRTI, which is itself an
//! aggregate of Coinbase/Kraken/Bitstamp/etc. spot prices — Coinbase spot is a
//! close, fast proxy.

pub mod nws;

use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use mb_core::{MarketEvent, RefPrice};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

pub const REST_BASE: &str = "https://api.exchange.coinbase.com";
pub const WS_URL: &str = "wss://ws-feed.exchange.coinbase.com";
pub const SOURCE: &str = "coinbase";

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Candle {
    /// Bucket start, unix seconds.
    pub ts: i64,
    pub low: f64,
    pub high: f64,
    pub open: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Clone)]
pub struct CoinbaseRest {
    http: reqwest::Client,
}

impl CoinbaseRest {
    pub fn new() -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .user_agent("marketsbot/0.1")
                .build()?,
        })
    }

    /// Up to 300 candles per call. `granularity` in seconds (60, 300, 900, 3600, ...).
    pub async fn candles(&self, product: &str, granularity: u32, start_ts: i64, end_ts: i64) -> Result<Vec<Candle>> {
        let to_iso = |t: i64| chrono::DateTime::from_timestamp(t, 0).map(|d| d.to_rfc3339()).unwrap_or_default();
        let mut attempt = 0;
        loop {
            attempt += 1;
            let resp = self
                .http
                .get(format!("{REST_BASE}/products/{product}/candles"))
                .query(&[
                    ("granularity", granularity.to_string()),
                    ("start", to_iso(start_ts)),
                    ("end", to_iso(end_ts)),
                ])
                .send()
                .await?;
            if resp.status().as_u16() == 429 && attempt < 6 {
                tokio::time::sleep(Duration::from_millis(500 * attempt)).await;
                continue;
            }
            let raw: Vec<Vec<f64>> = resp.error_for_status()?.json().await.context("coinbase candles")?;
            let mut out: Vec<Candle> = raw
                .into_iter()
                .filter(|r| r.len() >= 6)
                .map(|r| Candle {
                    ts: r[0] as i64,
                    low: r[1],
                    high: r[2],
                    open: r[3],
                    close: r[4],
                    volume: r[5],
                })
                .collect();
            out.sort_by_key(|c| c.ts);
            return Ok(out);
        }
    }

    /// Page through `[start_ts, end_ts)` in 300-candle chunks.
    pub async fn candles_range(&self, product: &str, granularity: u32, start_ts: i64, end_ts: i64) -> Result<Vec<Candle>> {
        let chunk = (granularity as i64) * 300;
        let mut out = Vec::new();
        let mut t = start_ts;
        while t < end_ts {
            let e = (t + chunk).min(end_ts);
            let c = self.candles(product, granularity, t, e).await?;
            out.extend(c);
            t = e;
            tokio::time::sleep(Duration::from_millis(120)).await; // public rate limit ~10 rps
        }
        out.sort_by_key(|c| c.ts);
        out.dedup_by_key(|c| c.ts);
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Tick {
    pub trade_id: i64,
    pub ts_ms: i64,
    pub px: f64,
    pub size: f64,
}

impl CoinbaseRest {
    /// One page (≤1000) of trades older than `after_id` (newest first). `None` = latest.
    pub async fn trades_page(&self, product: &str, after_id: Option<i64>) -> Result<Vec<Tick>> {
        #[derive(serde::Deserialize)]
        struct T {
            trade_id: i64,
            time: String,
            price: String,
            size: String,
        }
        let mut attempt = 0;
        loop {
            attempt += 1;
            let mut req = self
                .http
                .get(format!("{REST_BASE}/products/{product}/trades"))
                .query(&[("limit", "1000".to_string())]);
            if let Some(a) = after_id {
                req = req.query(&[("after", a.to_string())]);
            }
            let resp = req.send().await?;
            if (resp.status().as_u16() == 429 || resp.status().is_server_error()) && attempt < 8 {
                tokio::time::sleep(Duration::from_millis(400 * attempt)).await;
                continue;
            }
            let raw: Vec<T> = resp.error_for_status()?.json().await.context("coinbase trades")?;
            return Ok(raw
                .into_iter()
                .filter_map(|t| {
                    Some(Tick {
                        trade_id: t.trade_id,
                        ts_ms: chrono::DateTime::parse_from_rfc3339(&t.time).ok()?.timestamp_millis(),
                        px: t.price.parse().ok()?,
                        size: t.size.parse().ok()?,
                    })
                })
                .collect());
        }
    }

    /// All trades with `start_ms <= ts < end_ms`, oldest first, by paging
    /// backwards from the newest trade. ~10 requests/s (public rate limit).
    /// `progress` is called with (ticks so far, oldest ts seen).
    pub async fn trades_range(&self, product: &str, start_ms: i64, end_ms: i64, mut progress: impl FnMut(usize, i64)) -> Result<Vec<Tick>> {
        let mut out: Vec<Tick> = Vec::new();
        let mut after: Option<i64> = None;
        let mut n = 0usize;
        loop {
            let page = self.trades_page(product, after).await?;
            let Some(last) = page.last() else { break };
            let oldest = last.ts_ms;
            after = Some(last.trade_id);
            for t in page {
                if t.ts_ms >= start_ms && t.ts_ms < end_ms {
                    out.push(t);
                }
            }
            n += 1;
            if n % 50 == 0 {
                progress(out.len(), oldest);
            }
            if oldest < start_ms {
                break;
            }
            tokio::time::sleep(Duration::from_millis(105)).await;
        }
        out.sort_by_key(|t| t.trade_id);
        Ok(out)
    }
}

pub struct CoinbaseWs;

impl CoinbaseWs {
    /// Stream `ticker` updates for the given products as `MarketEvent::Ref`.
    pub async fn run(products: Vec<String>, tx: mpsc::Sender<MarketEvent>) -> Result<()> {
        let mut backoff = Duration::from_secs(1);
        loop {
            match Self::session(&products, &tx).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if tx.is_closed() {
                        return Ok(());
                    }
                    error!(error = %e, "coinbase ws error; reconnecting in {:?}", backoff);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        }
    }

    async fn session(products: &[String], tx: &mpsc::Sender<MarketEvent>) -> Result<()> {
        let (ws, _) = tokio_tungstenite::connect_async(WS_URL).await.context("coinbase ws connect")?;
        info!(?products, "coinbase ws connected");
        let (mut sink, mut stream) = ws.split();
        let sub = json!({ "type": "subscribe", "product_ids": products, "channels": ["ticker"] });
        sink.send(Message::Text(sub.to_string().into())).await?;
        while let Some(msg) = stream.next().await {
            match msg? {
                Message::Text(t) => {
                    let v: Value = match serde_json::from_str(t.as_str()) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(error = %e, "bad coinbase json");
                            continue;
                        }
                    };
                    if v.get("type").and_then(Value::as_str) != Some("ticker") {
                        continue;
                    }
                    let (Some(px), Some(sym)) = (
                        v.get("price").and_then(Value::as_str).and_then(|s| s.parse::<f64>().ok()),
                        v.get("product_id").and_then(Value::as_str),
                    ) else {
                        continue;
                    };
                    let ts_ms = v
                        .get("time")
                        .and_then(Value::as_str)
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                        .map(|d| d.timestamp_millis())
                        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                    let ev = MarketEvent::Ref(RefPrice {
                        source: SOURCE.into(),
                        symbol: sym.to_string(),
                        ts_ms,
                        px,
                        avg_60s: None,
                    });
                    if tx.send(ev).await.is_err() {
                        return Ok(());
                    }
                }
                Message::Ping(p) => sink.send(Message::Pong(p)).await?,
                Message::Close(c) => return Err(anyhow!("coinbase ws close: {c:?}")),
                _ => {}
            }
        }
        Err(anyhow!("coinbase ws ended"))
    }
}
