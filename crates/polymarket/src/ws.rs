//! Polymarket public market WebSocket → `MarketEvent` (ticker = token id).

use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use mb_core::{BookSide, Fp, MarketEvent, Outcome, Trade, Venue};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

pub struct PolymarketWs {
    url: String,
}

impl Default for PolymarketWs {
    fn default() -> Self {
        Self::new()
    }
}

impl PolymarketWs {
    pub fn new() -> Self {
        Self {
            url: crate::WS_MARKET.to_string(),
        }
    }

    pub async fn run(&self, token_ids: Vec<String>, tx: mpsc::Sender<MarketEvent>) -> Result<()> {
        let mut backoff = Duration::from_secs(1);
        loop {
            match self.session(&token_ids, &tx).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if tx.is_closed() {
                        return Ok(());
                    }
                    error!(error = %e, "polymarket ws error; reconnecting in {:?}", backoff);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        }
    }

    async fn session(&self, token_ids: &[String], tx: &mpsc::Sender<MarketEvent>) -> Result<()> {
        let (ws, _) = tokio_tungstenite::connect_async(&self.url).await.context("polymarket ws connect")?;
        info!(n = token_ids.len(), "polymarket ws connected");
        let (mut sink, mut stream) = ws.split();
        let sub = json!({ "assets_ids": token_ids, "type": "market" });
        sink.send(Message::Text(sub.to_string().into())).await?;

        let mut ping = tokio::time::interval(Duration::from_secs(10));
        ping.tick().await;
        loop {
            tokio::select! {
                _ = ping.tick() => { sink.send(Message::Text("PING".into())).await?; }
                msg = stream.next() => {
                    let msg = match msg {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => return Err(e.into()),
                        None => return Err(anyhow!("polymarket ws closed")),
                    };
                    if let Message::Text(t) = msg {
                        let s = t.as_str();
                        if s == "PONG" { continue; }
                        let v: Value = match serde_json::from_str(s) {
                            Ok(v) => v,
                            Err(e) => { warn!(error = %e, "bad polymarket ws json"); continue; }
                        };
                        let items: Vec<&Value> = match &v { Value::Array(a) => a.iter().collect(), other => vec![other] };
                        for item in items {
                            for ev in parse_message(item) {
                                if tx.send(ev).await.is_err() { return Ok(()); }
                            }
                        }
                    } else if let Message::Close(c) = msg {
                        return Err(anyhow!("polymarket ws close: {c:?}"));
                    }
                }
            }
        }
    }
}

fn fp(v: Option<&Value>) -> Option<Fp> {
    match v? {
        Value::String(s) => Fp::parse(s).ok(),
        Value::Number(n) => n.as_f64().map(Fp::from_f64),
        _ => None,
    }
}

fn ts_ms(v: &Value) -> i64 {
    match v.get("timestamp") {
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
        _ => chrono::Utc::now().timestamp_millis(),
    }
}

pub fn parse_message(v: &Value) -> Vec<MarketEvent> {
    let Some(et) = v.get("event_type").and_then(Value::as_str) else {
        return vec![];
    };
    let ts = ts_ms(v);
    match et {
        "book" => {
            let lv = |k: &str| -> Vec<(Fp, Fp)> {
                v.get(k)
                    .and_then(Value::as_array)
                    .map(|a| a.iter().filter_map(|l| Some((fp(l.get("price"))?, fp(l.get("size"))?))).collect())
                    .unwrap_or_default()
            };
            vec![MarketEvent::BookSnapshot {
                venue: Venue::Polymarket,
                ticker: v.get("asset_id").and_then(Value::as_str).unwrap_or("").to_string(),
                ts_ms: ts,
                seq: 0,
                bids: lv("bids"),
                asks: lv("asks"),
            }]
        }
        "price_change" => v
            .get("price_changes")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|c| {
                        let side = match c.get("side").and_then(Value::as_str) {
                            Some("BUY") => BookSide::Bid,
                            Some("SELL") => BookSide::Ask,
                            _ => return None,
                        };
                        Some(MarketEvent::BookLevel {
                            venue: Venue::Polymarket,
                            ticker: c.get("asset_id").and_then(Value::as_str)?.to_string(),
                            ts_ms: ts,
                            side,
                            px: fp(c.get("price"))?,
                            qty: fp(c.get("size"))?,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default(),
        "last_trade_price" => {
            let (Some(px), Some(qty)) = (fp(v.get("price")), fp(v.get("size"))) else {
                return vec![];
            };
            vec![MarketEvent::Trade(Trade {
                venue: Venue::Polymarket,
                ticker: v.get("asset_id").and_then(Value::as_str).unwrap_or("").to_string(),
                ts_ms: ts,
                yes_px: px,
                qty,
                taker: if v.get("side").and_then(Value::as_str) == Some("SELL") { Outcome::No } else { Outcome::Yes },
                trade_id: v.get("transaction_hash").and_then(Value::as_str).unwrap_or("").to_string(),
            })]
        }
        _ => vec![],
    }
}
