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
    // Verified against Kalshi's settled values by research/station_map.py; must stay in
    // step with the table in mb_strategy::weather_lock, which decides on these feeds.
    Some(match series {
        "KXHIGHAUS" => "KAUS",
        "KXHIGHCHI" => "KMDW",
        "KXHIGHDEN" => "KDEN",
        "KXHIGHLAX" => "KLAX",
        "KXHIGHMIA" => "KMIA",
        "KXHIGHPHIL" => "KPHL",
        "KXHIGHTATL" => "KATL",
        "KXHIGHTBOS" => "KBOS",
        "KXHIGHTDAL" => "KDFW",
        "KXHIGHTDC" => "KDCA",
        "KXHIGHTEWR" => "KEWR",
        "KXHIGHTHOU" => "KHOU",
        "KXHIGHTLV" => "KLAS",
        "KXHIGHTMIN" => "KMSP",
        "KXHIGHTNOLA" => "KMSY",
        "KXHIGHTOKC" => "KOKC",
        "KXHIGHTPHX" => "KPHX",
        "KXHIGHTSAN" => "KSAN",
        "KXHIGHTSATX" => "KSAT",
        "KXHIGHTSDF" => "KSDF",
        "KXHIGHTSEA" => "KSEA",
        "KXHIGHTSFO" => "KSFO",
        "KXHIGHTTTN" => "KTTN",
        "KXLOWTATL" => "KATL",
        "KXLOWTAUS" => "KAUS",
        "KXLOWTBOS" => "KBOS",
        "KXLOWTCHI" => "KMDW",
        "KXLOWTDAL" => "KDFW",
        "KXLOWTDC" => "KDCA",
        "KXLOWTDEN" => "KDEN",
        "KXLOWTEWR" => "KEWR",
        "KXLOWTHOU" => "KHOU",
        "KXLOWTLAX" => "KLAX",
        "KXLOWTLV" => "KLAS",
        "KXLOWTMIA" => "KMIA",
        "KXLOWTMIN" => "KMSP",
        "KXLOWTNOLA" => "KMSY",
        "KXLOWTNYC" => "KNYC",
        "KXLOWTOKC" => "KOKC",
        "KXLOWTPHIL" => "KPHL",
        "KXLOWTPHX" => "KPHX",
        "KXLOWTSAN" => "KSAN",
        "KXLOWTSATX" => "KSAT",
        "KXLOWTSDF" => "KSDF",
        "KXLOWTSEA" => "KSEA",
        "KXLOWTSFO" => "KSFO",
        "KXLOWTTTN" => "KTTN",
        _ => return None,
    })
}

/// KXRAIN city code (ticker suffix) → NWS station id.
pub const RAIN_STATIONS: &[(&str, &str)] = &[
    ("ATL", "KATL"), ("AUS", "KAUS"), ("BOS", "KBOS"), ("CHI", "KORD"), ("DAL", "KDFW"), ("DC", "KDCA"), ("DEN", "KDEN"), ("EWR", "KEWR"),
    ("HOU", "KIAH"), ("LAX", "KLAX"), ("LV", "KLAS"), ("MIA", "KMIA"), ("MIN", "KMSP"), ("NOLA", "KMSY"), ("NYC", "KNYC"), ("OKC", "KOKC"),
    ("PHIL", "KPHL"), ("PHX", "KPHX"), ("SATX", "KSAT"), ("SEA", "KSEA"), ("SFO", "KSFO"), ("TTN", "KTTN"),
];

pub fn rain_station(city: &str) -> Option<&'static str> {
    RAIN_STATIONS.iter().find(|(c, _)| *c == city).map(|(_, s)| *s)
}

pub async fn run(stations: Vec<String>, every: Duration, tx: mpsc::Sender<MarketEvent>) -> Result<()> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .user_agent("marketsbot (research; contact via github.com/suryacks/MarketsBot)")
        .build()?;
    let mut last_seen: HashMap<String, String> = HashMap::new();
    info!(?stations, every_secs = every.as_secs(), "nws observation feed");

    // Backfill the last 24 hours before polling forward. A settlement lock reasons about
    // the day's running extremes, and those are made once: the maximum in the afternoon,
    // the minimum at dawn. A process that starts at noon and only watches from then on has
    // not seen either, so it under-reads the max and over-reads the min -- safe, in that it
    // simply declines to trade, but it would sit out most of the day it was started.
    let since = (chrono::Utc::now() - chrono::Duration::hours(24)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    for s in &stations {
        let url = format!("https://api.weather.gov/stations/{s}/observations?start={since}&limit=200");
        match http.get(&url).header("Accept", "application/geo+json").send().await {
            Ok(r) if r.status().is_success() => {
                let Ok(v) = r.json::<serde_json::Value>().await else { continue };
                let mut obs: Vec<(i64, f64)> = v["features"]
                    .as_array()
                    .map(|fs| {
                        fs.iter()
                            .filter_map(|f| {
                                let p = &f["properties"];
                                let ts = chrono::DateTime::parse_from_rfc3339(p["timestamp"].as_str()?).ok()?.timestamp_millis();
                                Some((ts, p["temperature"]["value"].as_f64()? * 9.0 / 5.0 + 32.0))
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                obs.sort_by_key(|(ts, _)| *ts); // oldest first, so the day rollover lands correctly
                let n = obs.len();
                for (ts_ms, f) in obs {
                    let _ = tx.send(MarketEvent::Ref(RefPrice { source: SOURCE.into(), symbol: s.clone(), ts_ms, px: f, avg_60s: None })).await;
                }
                info!(station = %s, observations = n, "backfilled");
            }
            Ok(r) => warn!(station = %s, status = %r.status(), "nws backfill failed"),
            Err(e) => warn!(station = %s, error = %e, "nws backfill failed"),
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    loop {
        for s in &stations {
            let url = format!("https://api.weather.gov/stations/{s}/observations/latest");
            match http.get(&url).header("Accept", "application/geo+json").send().await {
                Ok(r) if r.status().is_success() => {
                    if let Ok(v) = r.json::<serde_json::Value>().await {
                        let p = &v["properties"];
                        let ts = p["timestamp"].as_str().unwrap_or("").to_string();
                        if last_seen.get(s) != Some(&ts) {
                            last_seen.insert(s.clone(), ts.clone());
                            let ts_ms = chrono::DateTime::parse_from_rfc3339(&ts).map(|d| d.timestamp_millis()).unwrap_or_else(|_| chrono::Utc::now().timestamp_millis());
                            if let Some(c) = p["temperature"]["value"].as_f64() {
                                let _ = tx
                                    .send(MarketEvent::Ref(RefPrice { source: SOURCE.into(), symbol: s.clone(), ts_ms, px: c * 9.0 / 5.0 + 32.0, avg_60s: None }))
                                    .await;
                            }
                            // precipitation: mm in the last hour (null → 0), plus a rain flag from the present-weather text
                            let mm = p["precipitationLastHour"]["value"].as_f64().unwrap_or(0.0).max(0.0);
                            let text = p["textDescription"].as_str().unwrap_or("").to_lowercase();
                            let raining = ["rain", "drizzle", "thunderstorm", "showers"].iter().any(|w| text.contains(w));
                            let _ = tx
                                .send(MarketEvent::Ref(RefPrice { source: SOURCE.into(), symbol: format!("{s}:precip_mm"), ts_ms, px: mm, avg_60s: Some(if raining { 1.0 } else { 0.0 }) }))
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
