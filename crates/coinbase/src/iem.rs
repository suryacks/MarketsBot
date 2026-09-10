//! Live temperature observations from Iowa State's IEM, in degrees Fahrenheit.
//!
//! This replaces api.weather.gov for the settlement-lock strategy. NWS publishes whole
//! degrees CELSIUS, so the Fahrenheit value derived from it can sit 0.9 F either side of the
//! truth — enough to read Atlanta's 89 F maximum as 89.6 and sell an 88-89 bucket that then
//! settled at 89. IEM publishes `tmpf` directly, in the same unit Kalshi settles in, and it
//! is the source the strategy was validated against. Reading the same numbers the study read
//! is the point: the feed and the evidence must be the same measurement.

use anyhow::Result;
use mb_core::{MarketEvent, RefPrice};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

pub const SOURCE: &str = "nws"; // the strategy keys observations off this name

/// Station -> IEM network, for the stations the temperature series settle on.
pub const STATION_NETWORK: &[(&str, &str)] = &[
    ("KATL", "GA_ASOS"), ("KAUS", "TX_ASOS"), ("KBOS", "MA_ASOS"), ("KDCA", "DC_ASOS"),
    ("KDEN", "CO_ASOS"), ("KDFW", "TX_ASOS"), ("KEWR", "NJ_ASOS"), ("KHOU", "TX_ASOS"),
    ("KLAS", "NV_ASOS"), ("KLAX", "CA_ASOS"), ("KMDW", "IL_ASOS"), ("KMIA", "FL_ASOS"),
    ("KMSP", "MN_ASOS"), ("KMSY", "LA_ASOS"), ("KNYC", "NY_ASOS"), ("KOKC", "OK_ASOS"),
    ("KPHL", "PA_ASOS"), ("KPHX", "AZ_ASOS"), ("KSAN", "CA_ASOS"), ("KSAT", "TX_ASOS"),
    ("KSDF", "KY_ASOS"), ("KSEA", "WA_ASOS"), ("KSFO", "CA_ASOS"), ("KTTN", "NJ_ASOS"),
];

pub fn network_for(station: &str) -> Option<&'static str> {
    STATION_NETWORK.iter().find(|(s, _)| *s == station).map(|(_, n)| *n)
}

/// IEM station ids drop the leading K that NWS uses.
fn short(station: &str) -> &str {
    station.strip_prefix('K').unwrap_or(station)
}

/// Poll the day's observations for each station and emit them as °F `Ref` events.
///
/// Fetches whole days rather than the latest reading, so a restart still sees the extremes
/// that were set before it started — the dawn minimum and the afternoon maximum are each
/// made once, and a process that only watches forward has missed whichever came first.
pub async fn run(stations: Vec<String>, every: Duration, tx: mpsc::Sender<MarketEvent>) -> Result<()> {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(45))
        .user_agent("marketsbot (research; contact via github.com/suryacks/MarketsBot)")
        .build()?;
    let wanted: HashSet<String> = stations.iter().map(|s| short(s).to_string()).collect();
    let mut networks: Vec<&str> = stations.iter().filter_map(|s| network_for(s)).collect();
    networks.sort_unstable();
    networks.dedup();
    info!(?stations, ?networks, "IEM observation feed (degrees F)");

    // Backfill the last 36 hours before polling forward. The day's extremes are each made
    // once -- the minimum at dawn, the maximum in the afternoon -- so a process that starts
    // at noon and only watches forward has already missed one of them.
    let now = chrono::Utc::now();
    let start = now - chrono::Duration::hours(36);
    for st in &stations {
        let Some(net) = network_for(st) else { continue };
        let url = format!(
            "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station={}&network={}&data=tmpf\
             &year1={}&month1={}&day1={}&year2={}&month2={}&day2={}&tz=Etc/UTC&format=onlycomma\
             &latlon=no&missing=M&trace=T&direct=no&report_type=2&report_type=3",
            short(st), net,
            start.format("%Y"), start.format("%m"), start.format("%d"),
            (now + chrono::Duration::days(1)).format("%Y"), (now + chrono::Duration::days(1)).format("%m"), (now + chrono::Duration::days(1)).format("%d"),
        );
        match http.get(&url).send().await {
            Ok(r) if r.status().is_success() => {
                let text = r.text().await.unwrap_or_default();
                let mut rows: Vec<(i64, f64)> = Vec::new();
                for line in text.lines() {
                    let cols: Vec<&str> = line.split(',').collect();
                    if cols.len() < 3 || cols[0] == "station" {
                        continue;
                    }
                    let (Ok(ts), Ok(f)) = (
                        chrono::NaiveDateTime::parse_from_str(&cols[1][..cols[1].len().min(16)], "%Y-%m-%d %H:%M"),
                        cols[2].parse::<f64>(),
                    ) else {
                        continue;
                    };
                    rows.push((ts.and_utc().timestamp_millis(), f));
                }
                rows.sort_by_key(|(t, _)| *t); // oldest first, so day rollovers land correctly
                let n = rows.len();
                for (ts_ms, f) in rows {
                    let _ = tx.send(MarketEvent::Ref(RefPrice { source: SOURCE.into(), symbol: st.clone(), ts_ms, px: f, avg_60s: None })).await;
                }
                info!(station = %st, observations = n, "IEM backfilled");
            }
            Ok(r) => warn!(station = %st, status = %r.status(), "IEM backfill failed"),
            Err(e) => warn!(station = %st, error = %e, "IEM backfill failed"),
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }

    let mut last_seen: HashMap<String, String> = HashMap::new();
    loop {
        for net in &networks {
            let url = format!("https://mesonet.agron.iastate.edu/api/1/currents.json?network={net}");
            match http.get(&url).send().await {
                Ok(r) if r.status().is_success() => match r.json::<serde_json::Value>().await {
                    Ok(v) => {
                        let rows = v["data"].as_array().cloned().unwrap_or_default();
                        for row in rows {
                            let Some(st) = row["station"].as_str() else { continue };
                            if !wanted.contains(st) {
                                continue;
                            }
                            let Some(f) = row["tmpf"].as_f64() else { continue };
                            let ts = row["utc_valid"].as_str().or_else(|| row["valid"].as_str()).unwrap_or("").to_string();
                            let key = format!("K{st}");
                            if last_seen.get(&key) == Some(&ts) {
                                continue;
                            }
                            last_seen.insert(key.clone(), ts.clone());
                            let ts_ms = chrono::DateTime::parse_from_rfc3339(&ts)
                                .map(|d| d.timestamp_millis())
                                .unwrap_or_else(|_| chrono::Utc::now().timestamp_millis());
                            let _ = tx
                                .send(MarketEvent::Ref(RefPrice { source: SOURCE.into(), symbol: key, ts_ms, px: f, avg_60s: None }))
                                .await;
                        }
                    }
                    Err(e) => warn!(network = %net, error = %e, "IEM parse failed"),
                },
                Ok(r) => warn!(network = %net, status = %r.status(), "IEM request failed"),
                Err(e) => warn!(network = %net, error = %e, "IEM request failed"),
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        tokio::time::sleep(every).await;
    }
}
