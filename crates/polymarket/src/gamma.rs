use anyhow::{Context, Result};
use mb_core::{FeeModel, MarketInfo, Venue};
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FeeSchedule {
    #[serde(default)]
    pub rate: f64,
    #[serde(default)]
    pub exponent: f64,
    #[serde(default)]
    pub taker_only: bool,
    #[serde(default)]
    pub rebate_rate: f64,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GammaMarket {
    pub id: String,
    #[serde(default)]
    pub question: String,
    #[serde(default)]
    pub slug: String,
    #[serde(default)]
    pub condition_id: String,
    /// JSON-encoded array string, e.g. "[\"1234\", \"5678\"]"
    #[serde(default)]
    pub clob_token_ids: String,
    #[serde(default)]
    pub outcomes: String,
    #[serde(default)]
    pub outcome_prices: String,
    #[serde(default)]
    pub group_item_title: String,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub closed: bool,
    #[serde(default)]
    pub accepting_orders: bool,
    #[serde(default)]
    pub neg_risk: bool,
    #[serde(default)]
    pub fees_enabled: bool,
    #[serde(default)]
    pub fee_schedule: Option<FeeSchedule>,
    #[serde(default)]
    pub order_price_min_tick_size: Option<f64>,
    #[serde(default)]
    pub order_min_size: Option<f64>,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub start_date: Option<String>,
    #[serde(default)]
    pub volume24hr: Option<f64>,
    #[serde(default)]
    pub liquidity_num: Option<f64>,
    #[serde(default)]
    pub best_bid: Option<f64>,
    #[serde(default)]
    pub best_ask: Option<f64>,
    #[serde(default)]
    pub last_trade_price: Option<f64>,
}

impl GammaMarket {
    fn parse_list(s: &str) -> Vec<String> {
        serde_json::from_str::<Vec<String>>(s).unwrap_or_default()
    }
    pub fn token_ids(&self) -> Vec<String> {
        Self::parse_list(&self.clob_token_ids)
    }
    pub fn outcome_names(&self) -> Vec<String> {
        Self::parse_list(&self.outcomes)
    }
    pub fn outcome_prices_f64(&self) -> Vec<f64> {
        Self::parse_list(&self.outcome_prices)
            .iter()
            .filter_map(|p| p.parse().ok())
            .collect()
    }
    /// Token id for the first outcome (YES for binary markets).
    pub fn yes_token(&self) -> Option<String> {
        self.token_ids().into_iter().next()
    }
    pub fn no_token(&self) -> Option<String> {
        self.token_ids().into_iter().nth(1)
    }
    pub fn fee_model(&self) -> FeeModel {
        if !self.fees_enabled {
            return FeeModel::None;
        }
        FeeModel::polymarket(self.fee_schedule.as_ref().map(|f| f.rate).unwrap_or(0.0))
    }
    fn ts(s: &Option<String>) -> i64 {
        s.as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.timestamp_millis())
            .unwrap_or(0)
    }
    /// Describe the YES token as a market (ticker = token id).
    pub fn to_info(&self, event_ticker: &str) -> Option<MarketInfo> {
        let yes = self.yes_token()?;
        Some(MarketInfo {
            venue: Venue::Polymarket.as_str().to_string(),
            ticker: yes,
            event_ticker: event_ticker.to_string(),
            series: String::new(),
            title: self.question.clone(),
            strike_type: String::new(),
            floor_strike: None,
            cap_strike: None,
            open_ts_ms: Self::ts(&self.start_date),
            close_ts_ms: Self::ts(&self.end_date),
            expiration_ts_ms: Self::ts(&self.end_date),
            status: if self.closed {
                "closed".into()
            } else if self.active {
                "active".into()
            } else {
                "inactive".into()
            },
            result: String::new(),
            settlement_value: None,
            yes_bid: self.best_bid.map(mb_core::Fp::from_f64),
            yes_ask: self.best_ask.map(mb_core::Fp::from_f64),
            volume: mb_core::Fp::from_f64(self.volume24hr.unwrap_or(0.0)),
            category: String::new(),
            open_px: None,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GammaEvent {
    pub id: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub slug: String,
    #[serde(default)]
    pub neg_risk: bool,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub closed: bool,
    #[serde(default)]
    pub end_date: Option<String>,
    #[serde(default)]
    pub volume24hr: Option<f64>,
    #[serde(default)]
    pub markets: Vec<GammaMarket>,
}

#[derive(Clone)]
pub struct GammaClient {
    http: reqwest::Client,
    base: String,
}

impl GammaClient {
    pub fn new() -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .user_agent("marketsbot/0.1")
                .build()?,
            base: crate::GAMMA_BASE.to_string(),
        })
    }

    /// Active events ordered by 24h volume (most liquid first).
    pub async fn events(&self, limit: u32, offset: u32) -> Result<Vec<GammaEvent>> {
        let url = format!("{}/events", self.base);
        self.http
            .get(&url)
            .query(&[
                ("limit", limit.to_string()),
                ("offset", offset.to_string()),
                ("active", "true".into()),
                ("closed", "false".into()),
                ("order", "volume24hr".into()),
                ("ascending", "false".into()),
            ])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("gamma events")
    }

    pub async fn markets(&self, limit: u32, offset: u32) -> Result<Vec<GammaMarket>> {
        let url = format!("{}/markets", self.base);
        self.http
            .get(&url)
            .query(&[
                ("limit", limit.to_string()),
                ("offset", offset.to_string()),
                ("active", "true".into()),
                ("closed", "false".into()),
                ("order", "volume24hr".into()),
                ("ascending", "false".into()),
            ])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("gamma markets")
    }

    pub async fn market_by_slug(&self, slug: &str) -> Result<GammaMarket> {
        let url = format!("{}/markets/slug/{slug}", self.base);
        self.http.get(&url).send().await?.error_for_status()?.json().await.context("gamma market")
    }

    pub async fn event(&self, id: &str) -> Result<GammaEvent> {
        let url = format!("{}/events/{id}", self.base);
        self.http.get(&url).send().await?.error_for_status()?.json().await.context("gamma event")
    }

    /// Simple text search over active markets (Gamma supports `?slug=` and tag
    /// filters, not free text, so this pages through and filters locally).
    pub async fn search(&self, needle: &str, max_pages: u32) -> Result<Vec<GammaMarket>> {
        let needle = needle.to_lowercase();
        let mut out = Vec::new();
        for page in 0..max_pages {
            let ms = self.markets(500, page * 500).await?;
            if ms.is_empty() {
                break;
            }
            out.extend(ms.into_iter().filter(|m| m.question.to_lowercase().contains(&needle)));
        }
        Ok(out)
    }
}
