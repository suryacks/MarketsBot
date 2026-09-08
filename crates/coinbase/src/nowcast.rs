//! Fast-moving weather inputs that lead the observation gauges (Open-Meteo, free, no key):
//!
//! * `minutely_15` precipitation — a radar-nowcast product: rain that will reach the station
//!   in the next 15–60 minutes, before any gauge records it. Emitted as
//!   `<STATION>:nowcast_precip_60m` (mm) with `avg_60s` = mm in the next 15 minutes.
//! * HRRR (`gfs_hrrr`) hourly temperature — a 3 km model that re-runs every hour, so its
//!   daily maximum updates far more often than the human forecast the market watches.
//!   Emitted as `<STATION>:hrrr_max_f` (°F, remaining maximum for the local day).
//!
//! These are *predictions*, not settled facts — strategies must treat them probabilistically.

use anyhow::Result;
use mb_core::{MarketEvent, RefPrice};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

pub const SOURCE: &str = "nowcast";

/// station -> (lat, lon, UTC offset of the local day)
pub const SITES: &[(&str, f64, f64, i32)] = &[
    ("KNYC", 40.7789, -73.9692, -4), ("KMDW", 41.7868, -87.7522, -5), ("KMIA", 25.7959, -80.2870, -4),
    ("KLAX", 33.9382, -118.3866, -7), ("KAUS", 30.1945, -97.6699, -5), ("KPHL", 39.8729, -75.2437, -4),
    ("KDEN", 39.8466, -104.6562, -6), ("KATL", 33.6301, -84.4418, -4), ("KBOS", 42.3606, -71.0097, -4),
    ("KORD", 41.9603, -87.9316, -5), ("KDFW", 32.8998, -97.0403, -5), ("KDCA", 38.8483, -77.0341, -4),
    ("KEWR", 40.6825, -74.1690, -4), ("KIAH", 29.9902, -95.3368, -5), ("KLAS", 36.0719, -115.1634, -7),
    ("KMSP", 44.8848, -93.2223, -5), ("KMSY", 29.9934, -90.2580, -5), ("KOKC", 35.3889, -97.6006, -5),
    ("KPHX", 33.4278, -112.0037, -7), ("KSAT", 29.5443, -98.4839, -5), ("KSEA", 47.4444, -122.3138, -7),
    ("KSFO", 37.6197, -122.3647, -7), ("KTTN", 40.2769, -74.8163, -4),
];

pub fn site(station: &str) -> Option<(f64, f64, i32)> {
    SITES.iter().find(|(s, ..)| *s == station).map(|(_, la, lo, off)| (*la, *lo, *off))
}

/// Poll the nowcast + HRRR for each station and emit Ref events.
pub async fn run(stations: Vec<String>, every: Duration, tx: mpsc::Sender<MarketEvent>) -> Result<()> {
    let http = reqwest::Client::builder().timeout(Duration::from_secs(25)).user_agent("marketsbot/0.1").build()?;
    info!(n = stations.len(), every_secs = every.as_secs(), "nowcast feed (radar 15-min precipitation + HRRR)");
    loop {
        for st in &stations {
            let Some((lat, lon, off)) = site(st) else { continue };
            let now_ms = chrono::Utc::now().timestamp_millis();
            // --- radar nowcast: mm of precipitation in the next 15 / 60 minutes ---
            let url = format!(
                "https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&minutely_15=precipitation&forecast_hours=3&timezone=UTC"
            );
            match http.get(&url).send().await {
                Ok(r) if r.status().is_success() => {
                    if let Ok(v) = r.json::<serde_json::Value>().await {
                        let vals: Vec<f64> = v["minutely_15"]["precipitation"].as_array().map(|a| a.iter().filter_map(|x| x.as_f64()).collect()).unwrap_or_default();
                        if !vals.is_empty() {
                            let next15 = vals.first().copied().unwrap_or(0.0);
                            let next60: f64 = vals.iter().take(4).sum();
                            let _ = tx
                                .send(MarketEvent::Ref(RefPrice { source: SOURCE.into(), symbol: format!("{st}:nowcast_precip_60m"), ts_ms: now_ms, px: next60, avg_60s: Some(next15) }))
                                .await;
                        }
                    }
                }
                Ok(r) => warn!(station = %st, status = %r.status(), "nowcast request failed"),
                Err(e) => warn!(station = %st, error = %e, "nowcast error"),
            }
            tokio::time::sleep(Duration::from_millis(600)).await;
            // --- HRRR: remaining maximum temperature for the local day ---
            let url = format!(
                "https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m&models=gfs_hrrr&temperature_unit=fahrenheit&forecast_days=2&timezone=UTC"
            );
            match http.get(&url).send().await {
                Ok(r) if r.status().is_success() => {
                    if let Ok(v) = r.json::<serde_json::Value>().await {
                        let times: Vec<String> = v["hourly"]["time"].as_array().map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect()).unwrap_or_default();
                        let temps: Vec<Option<f64>> = v["hourly"]["temperature_2m"].as_array().map(|a| a.iter().map(|x| x.as_f64()).collect()).unwrap_or_default();
                        // Emit the remaining maximum for TODAY and the full maximum for TOMORROW,
                        // each keyed by the local day so a strategy can match a market's day exactly.
                        // Local day key = floor((utc + off) / 86400), matching the strategies.
                        let now_s = now_ms / 1000;
                        let today = (now_s + off as i64 * 3600).div_euclid(86_400);
                        let mut maxes: std::collections::HashMap<i64, f64> = std::collections::HashMap::new();
                        for (t, temp) in times.iter().zip(temps.iter()) {
                            let Some(temp) = temp else { continue };
                            let Ok(dt) = chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M") else { continue };
                            let ts = dt.and_utc().timestamp();
                            let day = (ts + off as i64 * 3600).div_euclid(86_400);
                            // for today only the remaining hours matter; for later days, the whole day
                            if day == today && ts < now_s - 3600 {
                                continue;
                            }
                            let e = maxes.entry(day).or_insert(f64::MIN);
                            *e = e.max(*temp);
                        }
                        for (day, mx) in maxes {
                            if mx > f64::MIN {
                                let _ = tx
                                    .send(MarketEvent::Ref(RefPrice { source: SOURCE.into(), symbol: format!("{st}:hrrr_max_f:{day}"), ts_ms: now_ms, px: mx, avg_60s: None }))
                                    .await;
                            }
                        }
                    }
                }
                Ok(r) => warn!(station = %st, status = %r.status(), "hrrr request failed"),
                Err(e) => warn!(station = %st, error = %e, "hrrr error"),
            }
            tokio::time::sleep(Duration::from_millis(600)).await;
        }
        tokio::time::sleep(every).await;
    }
}
