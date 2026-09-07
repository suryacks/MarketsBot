use anyhow::{Context, Result};
use mb_core::{Fp, Orderbook};
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
pub struct Level {
    pub price: Fp,
    pub size: Fp,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClobBook {
    #[serde(default)]
    pub market: String,
    #[serde(default)]
    pub asset_id: String,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub hash: String,
    #[serde(default)]
    pub bids: Vec<Level>,
    #[serde(default)]
    pub asks: Vec<Level>,
    #[serde(default)]
    pub tick_size: Option<Fp>,
    #[serde(default)]
    pub min_order_size: Option<Fp>,
    #[serde(default)]
    pub neg_risk: bool,
    #[serde(default)]
    pub last_trade_price: Option<Fp>,
}

impl ClobBook {
    pub fn ts_ms(&self) -> i64 {
        self.timestamp.parse().unwrap_or(0)
    }
    /// Book in the token's own terms (for a YES token this is the YES book).
    pub fn to_orderbook(&self) -> Orderbook {
        let bids: Vec<(Fp, Fp)> = self.bids.iter().map(|l| (l.price, l.size)).collect();
        let asks: Vec<(Fp, Fp)> = self.asks.iter().map(|l| (l.price, l.size)).collect();
        let mut ob = Orderbook::new();
        ob.replace(&bids, &asks, self.ts_ms(), 0);
        ob
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct PricePoint {
    pub t: i64,
    pub p: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct History {
    #[serde(default)]
    history: Vec<PricePoint>,
}

#[derive(Clone)]
pub struct ClobClient {
    http: reqwest::Client,
    base: String,
}

impl ClobClient {
    pub fn new() -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .tcp_nodelay(true)
                .user_agent("marketsbot/0.1")
                .build()?,
            base: crate::CLOB_BASE.to_string(),
        })
    }

    pub async fn book(&self, token_id: &str) -> Result<ClobBook> {
        self.http
            .get(format!("{}/book", self.base))
            .query(&[("token_id", token_id)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("clob book")
    }

    pub async fn books(&self, token_ids: &[String]) -> Result<Vec<ClobBook>> {
        let body: Vec<serde_json::Value> = token_ids.iter().map(|t| serde_json::json!({ "token_id": t })).collect();
        self.http
            .post(format!("{}/books", self.base))
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("clob books")
    }

    pub async fn midpoint(&self, token_id: &str) -> Result<Fp> {
        #[derive(Deserialize)]
        struct R {
            mid: Fp,
        }
        let r: R = self
            .http
            .get(format!("{}/midpoint", self.base))
            .query(&[("token_id", token_id)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(r.mid)
    }

    /// Price history for a token. `fidelity` = sampling in minutes.
    pub async fn prices_history(&self, token_id: &str, start_ts: i64, end_ts: i64, fidelity: u32) -> Result<Vec<PricePoint>> {
        let r: History = self
            .http
            .get(format!("{}/prices-history", self.base))
            .query(&[
                ("market", token_id.to_string()),
                ("startTs", start_ts.to_string()),
                ("endTs", end_ts.to_string()),
                ("fidelity", fidelity.to_string()),
            ])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("clob prices-history")?;
        Ok(r.history)
    }
}
