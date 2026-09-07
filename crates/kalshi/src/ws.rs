//! Kalshi WebSocket feed → `MarketEvent`. Auth headers are required on the
//! handshake. Reconnects with backoff; resubscribes on reconnect.

use crate::auth::KalshiAuth;
use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use mb_core::{BookSide, Fp, MarketEvent, Outcome, Trade, Venue};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Channel {
    OrderbookDelta,
    Ticker,
    Trade,
    Fill,
    MarketLifecycle,
}

impl Channel {
    pub fn as_str(self) -> &'static str {
        match self {
            Channel::OrderbookDelta => "orderbook_delta",
            Channel::Ticker => "ticker",
            Channel::Trade => "trade",
            Channel::Fill => "fill",
            Channel::MarketLifecycle => "market_lifecycle_v2",
        }
    }
}

pub struct KalshiWs {
    url: String,
    auth: Arc<KalshiAuth>,
}

impl KalshiWs {
    pub fn new(url: &str, auth: KalshiAuth) -> Self {
        Self {
            url: url.to_string(),
            auth: Arc::new(auth),
        }
    }

    pub fn from_env() -> Result<Self> {
        let (_, ws) = crate::env_urls();
        let auth = KalshiAuth::from_env()?.ok_or_else(|| anyhow!("Kalshi WebSocket requires API keys"))?;
        Ok(Self::new(ws, auth))
    }

    /// Run forever (until `tx` closes), emitting normalized events.
    /// `tickers` empty ⇒ subscribe channel-wide where Kalshi allows it.
    pub async fn run(&self, channels: &[Channel], tickers: Vec<String>, tx: mpsc::Sender<MarketEvent>) -> Result<()> {
        let (_wtx, wrx) = tokio::sync::watch::channel(tickers);
        self.run_dynamic(channels, wrx, tx).await
    }

    /// Like `run`, but the ticker set can change over time (new 15-minute
    /// markets open continuously). New tickers get an additional subscribe
    /// command on the live connection; a reconnect resubscribes the full set.
    pub async fn run_dynamic(
        &self,
        channels: &[Channel],
        tickers: tokio::sync::watch::Receiver<Vec<String>>,
        tx: mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        let mut backoff = Duration::from_secs(1);
        loop {
            match self.session(channels, tickers.clone(), &tx).await {
                Ok(()) => {
                    info!("kalshi ws session ended cleanly");
                    return Ok(());
                }
                Err(e) => {
                    if tx.is_closed() {
                        return Ok(());
                    }
                    error!(error = %e, "kalshi ws session error; reconnecting in {:?}", backoff);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
        }
    }

    async fn session(
        &self,
        channels: &[Channel],
        mut tickers: tokio::sync::watch::Receiver<Vec<String>>,
        tx: &mpsc::Sender<MarketEvent>,
    ) -> Result<()> {
        let path = url::Url::parse(&self.url)?.path().to_string();
        let mut req = self.url.as_str().into_client_request()?;
        for (k, v) in self.auth.headers("GET", &path) {
            req.headers_mut().insert(k, v.parse()?);
        }
        let (ws, _) = tokio_tungstenite::connect_async(req).await.context("kalshi ws connect")?;
        info!(url = %self.url, "kalshi ws connected");
        let (mut sink, mut stream) = ws.split();

        let chans: Vec<&str> = channels.iter().map(|c| c.as_str()).collect();
        let mut cmd_id = 0u64;
        let mut subscribed: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut subscribe = |ts: &[String]| {
            cmd_id += 1;
            let mut params = json!({ "channels": chans });
            if !ts.is_empty() {
                params["market_tickers"] = json!(ts);
            }
            json!({ "id": cmd_id, "cmd": "subscribe", "params": params }).to_string()
        };

        let initial = tickers.borrow_and_update().clone();
        subscribed.extend(initial.iter().cloned());
        let msg = subscribe(&initial);
        sink.send(Message::Text(msg.into())).await?;
        debug!(n = initial.len(), "kalshi ws subscribed");

        let mut ping = tokio::time::interval(Duration::from_secs(10));
        ping.tick().await;
        loop {
            tokio::select! {
                _ = ping.tick() => {
                    sink.send(Message::Ping(Vec::new().into())).await?;
                }
                changed = tickers.changed() => {
                    if changed.is_err() { return Ok(()); }
                    let cur = tickers.borrow_and_update().clone();
                    let new: Vec<String> = cur.iter().filter(|t| !subscribed.contains(*t)).cloned().collect();
                    if !new.is_empty() {
                        subscribed.extend(new.iter().cloned());
                        let msg = subscribe(&new);
                        sink.send(Message::Text(msg.into())).await?;
                        info!(?new, "kalshi ws added subscriptions");
                    }
                }
                msg = stream.next() => {
                    let msg = match msg {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => return Err(e.into()),
                        None => return Err(anyhow!("kalshi ws closed")),
                    };
                    match msg {
                        Message::Text(t) => {
                            let v: Value = match serde_json::from_str(t.as_str()) {
                                Ok(v) => v,
                                Err(e) => { warn!(error = %e, "bad kalshi ws json"); continue; }
                            };
                            for ev in parse_message(&v) {
                                if tx.send(ev).await.is_err() { return Ok(()); }
                            }
                        }
                        Message::Ping(p) => { sink.send(Message::Pong(p)).await?; }
                        Message::Close(c) => return Err(anyhow!("kalshi ws close frame: {c:?}")),
                        _ => {}
                    }
                }
            }
        }
    }
}

fn fp(v: &Value) -> Option<Fp> {
    match v {
        Value::String(s) => Fp::parse(s).ok(),
        Value::Number(n) => n.as_f64().map(Fp::from_f64),
        _ => None,
    }
}

fn ts_ms(m: &Value) -> i64 {
    m.get("ts_ms")
        .and_then(Value::as_i64)
        .or_else(|| m.get("ts").and_then(Value::as_i64).map(|s| s * 1000))
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis())
}

/// Parse one Kalshi WS frame into zero or more normalized events.
pub fn parse_message(v: &Value) -> Vec<MarketEvent> {
    let Some(typ) = v.get("type").and_then(Value::as_str) else {
        return vec![];
    };
    let seq = v.get("seq").and_then(Value::as_i64).unwrap_or(0);
    let Some(m) = v.get("msg") else {
        match typ {
            "subscribed" | "ok" => debug!(?v, "kalshi ws ack"),
            "error" => warn!(?v, "kalshi ws error"),
            _ => {}
        }
        return vec![];
    };
    let ticker = m.get("market_ticker").and_then(Value::as_str).unwrap_or("").to_string();
    match typ {
        "orderbook_snapshot" => {
            let levels = |k: &str| -> Vec<(Fp, Fp)> {
                m.get(k)
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(|l| {
                                let l = l.as_array()?;
                                Some((fp(l.first()?)?, fp(l.get(1)?)?))
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            };
            let bids = levels("yes_dollars_fp");
            let asks: Vec<(Fp, Fp)> = levels("no_dollars_fp").into_iter().map(|(p, q)| (p.complement(), q)).collect();
            vec![MarketEvent::BookSnapshot {
                venue: Venue::Kalshi,
                ticker,
                ts_ms: ts_ms(m),
                seq,
                bids,
                asks,
            }]
        }
        "orderbook_delta" => {
            let (Some(px), Some(delta)) = (m.get("price_dollars").and_then(fp), m.get("delta_fp").and_then(fp)) else {
                return vec![];
            };
            let (side, px) = match m.get("side").and_then(Value::as_str) {
                Some("no") => (BookSide::Ask, px.complement()),
                _ => (BookSide::Bid, px),
            };
            vec![MarketEvent::BookDelta {
                venue: Venue::Kalshi,
                ticker,
                ts_ms: ts_ms(m),
                seq,
                side,
                px,
                delta,
            }]
        }
        "ticker" => vec![MarketEvent::Ticker {
            venue: Venue::Kalshi,
            ticker,
            ts_ms: ts_ms(m),
            yes_bid: m.get("yes_bid_dollars").and_then(fp),
            yes_ask: m.get("yes_ask_dollars").and_then(fp),
            last: m.get("price_dollars").and_then(fp),
        }],
        "fill" => {
            let (Some(px), Some(qty)) = (m.get("yes_price_dollars").and_then(fp), m.get("count_fp").and_then(fp)) else {
                return vec![];
            };
            let action = m.get("action").and_then(Value::as_str).unwrap_or("buy");
            let side = m
                .get("purchased_side")
                .or_else(|| m.get("outcome_side"))
                .or_else(|| m.get("side"))
                .and_then(Value::as_str)
                .unwrap_or("yes");
            // buying YES or selling NO both increase YES exposure
            let buy_yes = matches!((action, side), ("buy", "yes") | ("sell", "no"));
            vec![MarketEvent::UserFill(mb_core::UserFill {
                venue: Venue::Kalshi,
                ticker,
                ts_ms: ts_ms(m),
                trade_id: m.get("trade_id").and_then(Value::as_str).unwrap_or("").to_string(),
                order_id: m.get("order_id").and_then(Value::as_str).unwrap_or("").to_string(),
                buy_yes,
                yes_px: px,
                qty,
                fee: m.get("fee_cost").and_then(fp).unwrap_or(Fp::ZERO),
                is_taker: m.get("is_taker").and_then(Value::as_bool).unwrap_or(false),
            })]
        }
        "trade" => {
            let (Some(px), Some(qty)) = (m.get("yes_price_dollars").and_then(fp), m.get("count_fp").and_then(fp)) else {
                return vec![];
            };
            let side = m
                .get("taker_outcome_side")
                .or_else(|| m.get("taker_side"))
                .and_then(Value::as_str)
                .unwrap_or("yes");
            vec![MarketEvent::Trade(Trade {
                venue: Venue::Kalshi,
                ticker,
                ts_ms: ts_ms(m),
                yes_px: px,
                qty,
                taker: if side == "no" { Outcome::No } else { Outcome::Yes },
                trade_id: m.get("trade_id").and_then(Value::as_str).unwrap_or("").to_string(),
            })]
        }
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_snapshot_and_delta() {
        let snap = json!({"type":"orderbook_snapshot","sid":2,"seq":2,"msg":{
            "market_ticker":"T","yes_dollars_fp":[["0.0800","300.00"]],"no_dollars_fp":[["0.5400","20.00"]]}});
        let ev = parse_message(&snap);
        match &ev[0] {
            MarketEvent::BookSnapshot { bids, asks, .. } => {
                assert_eq!(bids[0], (Fp::parse("0.08").unwrap(), Fp::from_int(300)));
                assert_eq!(asks[0], (Fp::parse("0.46").unwrap(), Fp::from_int(20)));
            }
            _ => panic!(),
        }
        let delta = json!({"type":"orderbook_delta","seq":3,"msg":{"market_ticker":"T","price_dollars":"0.960","delta_fp":"-54.00","side":"no","ts_ms":1}});
        match &parse_message(&delta)[0] {
            MarketEvent::BookDelta { side, px, delta, .. } => {
                assert_eq!(*side, BookSide::Ask);
                assert_eq!(*px, Fp::parse("0.04").unwrap());
                assert_eq!(*delta, Fp::from_int(-54));
            }
            _ => panic!(),
        }
    }
}
