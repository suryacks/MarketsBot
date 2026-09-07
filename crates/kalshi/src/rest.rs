use crate::auth::KalshiAuth;
use crate::types::*;
use anyhow::{anyhow, Context, Result};
use mb_core::{Fp, Orderbook};
use reqwest::{Method, StatusCode};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

#[derive(Clone)]
pub struct KalshiClient {
    http: reqwest::Client,
    base: String,
    /// Path prefix of `base` (e.g. `/trade-api/v2`) — signed together with the endpoint path.
    base_path: String,
    auth: Option<Arc<KalshiAuth>>,
}

#[derive(Debug, Clone, Default)]
pub struct MarketsQuery {
    pub series_ticker: Option<String>,
    pub event_ticker: Option<String>,
    pub tickers: Option<Vec<String>>,
    /// unopened | open | paused | closed | settled
    pub status: Option<String>,
    pub min_close_ts: Option<i64>,
    pub max_close_ts: Option<i64>,
    pub min_settled_ts: Option<i64>,
    pub max_settled_ts: Option<i64>,
    pub limit: Option<u32>,
}

impl KalshiClient {
    pub fn new(base: &str, auth: Option<KalshiAuth>) -> Result<Self> {
        let url = url::Url::parse(base).context("kalshi base url")?;
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(16)
            .tcp_nodelay(true)
            .user_agent("marketsbot/0.1")
            .build()?;
        Ok(Self {
            http,
            base: base.trim_end_matches('/').to_string(),
            base_path: url.path().trim_end_matches('/').to_string(),
            auth: auth.map(Arc::new),
        })
    }

    /// Public-only client against production (no keys needed).
    pub fn public_prod() -> Result<Self> {
        Self::new(crate::PROD_REST, None)
    }

    /// From env: `KALSHI_ENV`, `KALSHI_API_KEY_ID`, `KALSHI_PRIVATE_KEY_PATH`.
    pub fn from_env() -> Result<Self> {
        let (rest, _) = crate::env_urls();
        Self::new(rest, KalshiAuth::from_env()?)
    }

    pub fn auth(&self) -> Option<&KalshiAuth> {
        self.auth.as_deref()
    }
    pub fn is_authenticated(&self) -> bool {
        self.auth.is_some()
    }
    pub fn base(&self) -> &str {
        &self.base
    }

    async fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<&impl Serialize>,
    ) -> Result<T> {
        let url = format!("{}{}", self.base, path);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let mut req = self.http.request(method.clone(), &url).query(query);
            if let Some(a) = &self.auth {
                for (k, v) in a.headers(method.as_str(), &format!("{}{}", self.base_path, path)) {
                    req = req.header(k, v);
                }
            }
            if let Some(b) = body {
                req = req.json(b);
            }
            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) if attempt < 5 => {
                    warn!(attempt, error = %e, path, "kalshi request error, retrying");
                    tokio::time::sleep(Duration::from_millis(200 * (1 << attempt))).await;
                    continue;
                }
                Err(e) => return Err(e).with_context(|| format!("{method} {path}")),
            };
            let status = resp.status();
            if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                if attempt < 6 {
                    let wait = Duration::from_millis(250 * (1 << attempt));
                    warn!(%status, attempt, path, "kalshi throttled/5xx, backing off {:?}", wait);
                    tokio::time::sleep(wait).await;
                    continue;
                }
            }
            if !status.is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(anyhow!("kalshi {method} {path} -> {status}: {text}"));
            }
            let bytes = resp.bytes().await?;
            return serde_json::from_slice::<T>(&bytes)
                .with_context(|| format!("decoding {path}: {}", String::from_utf8_lossy(&bytes[..bytes.len().min(400)])));
        }
    }

    async fn get<T: DeserializeOwned>(&self, path: &str, query: &[(&str, String)]) -> Result<T> {
        self.request(Method::GET, path, query, None::<&()>).await
    }

    // ---------- public market data ----------

    pub async fn get_markets(&self, q: &MarketsQuery, cursor: Option<&str>) -> Result<MarketsResponse> {
        let mut qs: Vec<(&str, String)> = vec![("limit", q.limit.unwrap_or(1000).to_string())];
        if let Some(s) = &q.series_ticker {
            qs.push(("series_ticker", s.clone()));
            qs.push(("mve_filter", "exclude".into()));
        }
        if let Some(e) = &q.event_ticker {
            qs.push(("event_ticker", e.clone()));
        }
        if let Some(t) = &q.tickers {
            qs.push(("tickers", t.join(",")));
        }
        if let Some(s) = &q.status {
            qs.push(("status", s.clone()));
        }
        for (k, v) in [
            ("min_close_ts", q.min_close_ts),
            ("max_close_ts", q.max_close_ts),
            ("min_settled_ts", q.min_settled_ts),
            ("max_settled_ts", q.max_settled_ts),
        ] {
            if let Some(v) = v {
                qs.push((k, v.to_string()));
            }
        }
        if let Some(c) = cursor {
            qs.push(("cursor", c.to_string()));
        }
        self.get("/markets", &qs).await
    }

    /// Follow pagination until exhausted.
    pub async fn get_all_markets(&self, q: &MarketsQuery) -> Result<Vec<Market>> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let r = self.get_markets(q, cursor.as_deref()).await?;
            let n = r.markets.len();
            out.extend(r.markets);
            debug!(n, total = out.len(), "markets page");
            if r.cursor.is_empty() || n == 0 {
                break;
            }
            cursor = Some(r.cursor);
        }
        Ok(out)
    }

    pub async fn get_market(&self, ticker: &str) -> Result<Market> {
        let r: MarketResponse = self.get(&format!("/markets/{ticker}"), &[]).await?;
        Ok(r.market)
    }

    pub async fn get_series(&self, series: &str) -> Result<Series> {
        let r: SeriesResponse = self.get(&format!("/series/{series}"), &[]).await?;
        Ok(r.series)
    }

    pub async fn list_series(&self, category: Option<&str>) -> Result<Vec<Series>> {
        let mut qs = vec![("limit", "500".to_string())];
        if let Some(c) = category {
            qs.push(("category", c.to_string()));
        }
        let r: SeriesListResponse = self.get("/series", &qs).await?;
        Ok(r.series)
    }

    /// Requires auth. Returns a YES-normalized book.
    pub async fn get_orderbook(&self, ticker: &str, depth: u32) -> Result<Orderbook> {
        let r: OrderbookResponse = self
            .get(&format!("/markets/{ticker}/orderbook"), &[("depth", depth.to_string())])
            .await?;
        let now = chrono::Utc::now().timestamp_millis();
        Ok(Orderbook::from_kalshi(&r.orderbook_fp.yes_dollars, &r.orderbook_fp.no_dollars, now, 0))
    }

    pub async fn get_trades(
        &self,
        ticker: Option<&str>,
        min_ts: Option<i64>,
        max_ts: Option<i64>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<TradesResponse> {
        let mut qs: Vec<(&str, String)> = vec![("limit", limit.to_string())];
        if let Some(t) = ticker {
            qs.push(("ticker", t.to_string()));
        }
        if let Some(t) = min_ts {
            qs.push(("min_ts", t.to_string()));
        }
        if let Some(t) = max_ts {
            qs.push(("max_ts", t.to_string()));
        }
        if let Some(c) = cursor {
            qs.push(("cursor", c.to_string()));
        }
        self.get("/markets/trades", &qs).await
    }

    /// Entire trade tape for one market (newest first as returned by the API).
    pub async fn get_all_trades(&self, ticker: &str) -> Result<Vec<TradeRec>> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let r = self.get_trades(Some(ticker), None, None, cursor.as_deref(), 1000).await?;
            let n = r.trades.len();
            out.extend(r.trades);
            if r.cursor.is_empty() || n == 0 {
                break;
            }
            cursor = Some(r.cursor);
        }
        Ok(out)
    }

    /// `period_interval`: 1 (minute), 60 (hour), 1440 (day). Timestamps in seconds.
    pub async fn get_candlesticks(
        &self,
        series: &str,
        ticker: &str,
        start_ts: i64,
        end_ts: i64,
        period_interval: u32,
    ) -> Result<Vec<Candle>> {
        let r: CandlesResponse = self
            .get(
                &format!("/series/{series}/markets/{ticker}/candlesticks"),
                &[
                    ("start_ts", start_ts.to_string()),
                    ("end_ts", end_ts.to_string()),
                    ("period_interval", period_interval.to_string()),
                ],
            )
            .await?;
        Ok(r.candlesticks)
    }

    // ---------- authenticated portfolio / trading ----------

    fn require_auth(&self) -> Result<()> {
        if self.auth.is_none() {
            return Err(anyhow!("this Kalshi endpoint requires KALSHI_API_KEY_ID / KALSHI_PRIVATE_KEY_PATH"));
        }
        Ok(())
    }

    pub async fn get_balance(&self) -> Result<serde_json::Value> {
        self.require_auth()?;
        self.get("/portfolio/balance", &[]).await
    }

    pub async fn get_positions(&self) -> Result<serde_json::Value> {
        self.require_auth()?;
        self.get("/portfolio/positions", &[("limit", "1000".into())]).await
    }

    pub async fn create_order(&self, req: &CreateOrderRequest) -> Result<CreateOrderResponse> {
        self.require_auth()?;
        self.request(Method::POST, "/portfolio/events/orders", &[], Some(req)).await
    }

    pub async fn cancel_order(&self, order_id: &str) -> Result<serde_json::Value> {
        self.require_auth()?;
        self.request(Method::DELETE, &format!("/portfolio/events/orders/{order_id}"), &[], None::<&()>)
            .await
    }

    pub async fn get_orders(&self, ticker: Option<&str>, status: Option<&str>) -> Result<serde_json::Value> {
        self.require_auth()?;
        let mut qs = vec![("limit", "200".to_string())];
        if let Some(t) = ticker {
            qs.push(("ticker", t.to_string()));
        }
        if let Some(s) = status {
            qs.push(("status", s.to_string()));
        }
        self.get("/portfolio/orders", &qs).await
    }
}

/// Helper to build a limit order in YES terms.
pub fn order_request(
    ticker: &str,
    action: mb_core::Action,
    yes_px: Fp,
    qty: Fp,
    tif: mb_core::Tif,
    post_only: bool,
    client_order_id: Option<String>,
) -> CreateOrderRequest {
    CreateOrderRequest {
        ticker: ticker.to_string(),
        side: match action {
            mb_core::Action::Buy => "bid".into(),
            mb_core::Action::Sell => "ask".into(),
        },
        count: qty.fmt_dec(2),
        price: yes_px.fmt_dec(4),
        time_in_force: match tif {
            mb_core::Tif::Ioc => "immediate_or_cancel".into(),
            mb_core::Tif::Gtc => "good_till_canceled".into(),
        },
        self_trade_prevention_type: "taker_at_cross".into(),
        client_order_id,
        post_only: if post_only { Some(true) } else { None },
        reduce_only: None,
        expiration_time: None,
    }
}
