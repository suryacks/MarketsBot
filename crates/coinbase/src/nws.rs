//! NWS observation feed (api.weather.gov, free, no key): polls the latest
//! observation for each station and emits `MarketEvent::Ref` with the
//! temperature in °F (source "nws", symbol = station id, e.g. "KNYC").

use anyhow::Result;
use mb_core::{MarketEvent, RefPrice};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

pub const SOURCE: &str = "nws";

/// Kalshi daily-high series → NWS/ASOS station id.
pub fn station_for(series: &str) -> Option<&'static str> {
    Some(match series {
        "KXHIGHNY" => "KNYC",
        "KXHIGHCHI" => "KMDW",
        "KXHIGHMIA" => "KMIA",
        "KXHIGHLAX" => "KLAX",
        "KXHIGHAUS" => "KAUS",
        "KXHIGHPHIL" => "KPHL",
        "KXHIGHDEN" => "KDEN",
        _ => return None,
    })
}

pub async fn run(stations: Vec<String>, every: Duration, tx: mpsc::Sender<MarketEvent>) -> Result<()> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .user_agent("marketsbot (research; contact via github.com/suryacks/MarketsBot)")
        .build()?;
    let mut last_seen: HashMap<String, String> = HashMap::new();
    info!(?stations, every_secs = every.as_secs(), "nws observation feed");
    loop {
        for s in &stations {
            let url = format!("https://api.weather.gov/stations/{s}/observations/latest");
            match http.get(&url).header("Accept", "application/geo+json").send().await {
                Ok(r) if r.status().is_success() => {
                    if let Ok(v) = r.json::<serde_json::Value>().await {
                        let p = &v["properties"];
                        let ts = p["timestamp"].as_str().unwrap_or("").to_string();
                        let temp_c = p["temperature"]["value"].as_f64();
                        if let Some(c) = temp_c
                            && last_seen.get(s) != Some(&ts)
                        {
                            last_seen.insert(s.clone(), ts.clone());
                            let ts_ms = chrono::DateTime::parse_from_rfc3339(&ts).map(|d| d.timestamp_millis()).unwrap_or_else(|_| chrono::Utc::now().timestamp_millis());
                            let f = c * 9.0 / 5.0 + 32.0;
                            let _ = tx
                                .send(MarketEvent::Ref(RefPrice {
                                    source: SOURCE.into(),
                                    symbol: s.clone(),
                                    ts_ms,
                                    px: f,
                                    avg_60s: None,
                                }))
                                .await;
                        }
                    }
                }
                Ok(r) => warn!(station = %s, status = %r.status(), "nws observation request failed"),
                Err(e) => warn!(station = %s, error = %e, "nws observation error"),
            }
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
        tokio::time::sleep(every).await;
    }
}
