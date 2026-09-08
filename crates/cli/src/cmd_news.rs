//! News sentiment features from GDELT (free, historical, no key): for each
//! market in news-driven categories, the average tone and article volume in the
//! 24 h before each long horizon, plus the same for the previous 24 h. Written to
//! `data/dataset/news.parquet`; `mbot universe` joins it on (ticker, horizon).
//!
//! GDELT DOC 2.0 API: https://api.gdeltproject.org/api/v2/doc/doc — paced at ~1 req/s.

use anyhow::Result;
use clap::Args as ClapArgs;
use mb_data::{read_dir, write_parquet, DsMarket};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NewsRow {
    pub ticker: String,
    pub horizon_secs: i64,
    pub query: String,
    /// mean article tone over the 24 h before the horizon (GDELT tone, roughly −10..+10)
    pub tone: f64,
    /// mean tone over the 24 h before that
    pub tone_prev: f64,
    pub articles: f64,
    pub articles_prev: f64,
}

#[derive(ClapArgs, Debug)]
pub struct Args {
    #[arg(long, default_value = "data/dataset")]
    pub data: PathBuf,
    #[arg(long = "category", default_values_t = vec!["Politics".to_string(), "Elections".to_string(), "Mentions".to_string(), "World".to_string(), "Companies".to_string(), "Economics".to_string(), "Science and Technology".to_string(), "Entertainment".to_string()])]
    pub categories: Vec<String>,
    /// Max markets to query (highest volume first)
    #[arg(long, default_value_t = 1200)]
    pub max_markets: usize,
    #[arg(long = "horizon", default_values_t = vec![6 * 3600i64, 24 * 3600])]
    pub horizons: Vec<i64>,
    #[arg(long, default_value_t = 1100)]
    pub pace_ms: u64,
}

const STOP: &[&str] = &[
    "will", "the", "a", "an", "of", "to", "in", "on", "at", "by", "for", "be", "is", "are", "and", "or", "than", "more", "less", "over", "under", "before", "after", "with", "from", "into", "up", "down", "this", "that", "who", "what", "when", "which", "how", "does", "do", "did", "say", "says", "said", "mention", "mentions", "yes", "no", "market", "price", "above", "below", "greater", "least", "between", "any", "next", "day", "week", "month", "year", "2026", "2025", "2027",
];

fn query_for(title: &str) -> Option<String> {
    let mut words: Vec<String> = title
        .split(|c: char| !c.is_alphanumeric() && c != '\'' && c != '-')
        .map(|w| w.trim_matches(|c: char| c == '\'' || c == '-').to_string())
        .filter(|w| w.len() >= 3 && !STOP.contains(&w.to_lowercase().as_str()) && !w.chars().all(|c| c.is_ascii_digit()))
        .collect();
    words.dedup();
    // prefer capitalized tokens (names), then longest
    words.sort_by_key(|w| (!w.chars().next().map(|c| c.is_uppercase()).unwrap_or(false), std::cmp::Reverse(w.len())));
    let picked: Vec<String> = words.into_iter().take(3).collect();
    if picked.len() < 2 {
        return None;
    }
    Some(picked.join(" "))
}

fn gdelt_ts(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0).map(|d| d.format("%Y%m%d%H%M%S").to_string()).unwrap_or_default()
}

async fn timeline(client: &reqwest::Client, q: &str, start: i64, end: i64, mode: &str) -> Result<Vec<(i64, f64)>> {
    #[derive(Deserialize)]
    struct Pt {
        date: String,
        value: f64,
    }
    #[derive(Deserialize)]
    struct Series {
        data: Vec<Pt>,
    }
    #[derive(Deserialize)]
    struct Resp {
        #[serde(default)]
        timeline: Vec<Series>,
    }
    let params: Vec<(&str, String)> = vec![
        ("query", q.to_string()),
        ("mode", mode.to_string()),
        ("format", "json".to_string()),
        ("startdatetime", gdelt_ts(start)),
        ("enddatetime", gdelt_ts(end)),
    ];
    let r = client.get("https://api.gdeltproject.org/api/v2/doc/doc").query(&params).send().await?;
    if !r.status().is_success() {
        anyhow::bail!("gdelt {}", r.status());
    }
    let text = r.text().await?;
    let resp: Resp = serde_json::from_str(&text).map_err(|e| anyhow::anyhow!("gdelt json: {e}: {}", text.chars().take(120).collect::<String>()))?;
    let mut out = Vec::new();
    if let Some(s) = resp.timeline.first() {
        for p in &s.data {
            // dates look like 20260901T120000Z
            if let Ok(d) = chrono::NaiveDateTime::parse_from_str(&p.date, "%Y%m%dT%H%M%SZ") {
                out.push((d.and_utc().timestamp(), p.value));
            }
        }
    }
    Ok(out)
}

fn mean_in(pts: &[(i64, f64)], lo: i64, hi: i64) -> Option<f64> {
    let v: Vec<f64> = pts.iter().filter(|(t, _)| *t >= lo && *t < hi).map(|(_, x)| *x).collect();
    if v.is_empty() { None } else { Some(v.iter().sum::<f64>() / v.len() as f64) }
}

pub async fn run(a: Args) -> Result<()> {
    let markets: Vec<DsMarket> = read_dir(a.data.join("markets"))?;
    let mut chosen: Vec<&DsMarket> = markets.iter().filter(|m| a.categories.contains(&m.category) && m.close_ts - m.open_ts >= 2 * 86_400).collect();
    chosen.sort_by(|x, y| y.volume.partial_cmp(&x.volume).unwrap());
    chosen.truncate(a.max_markets);
    let out_path = a.data.join("news.parquet");
    let mut rows: Vec<NewsRow> = if out_path.exists() { mb_data::read_parquet(&out_path)? } else { Vec::new() };
    let done: HashSet<(String, i64)> = rows.iter().map(|r| (r.ticker.clone(), r.horizon_secs)).collect();
    let client = reqwest::Client::builder().timeout(Duration::from_secs(30)).user_agent("marketsbot-research").build()?;
    info!(markets = chosen.len(), existing = rows.len(), "news features");
    // One GDELT lookup per (query, day) — every strike of an economics ladder shares its event's news.
    let mut cache: std::collections::HashMap<(String, i64), Option<(f64, f64, f64, f64)>> = std::collections::HashMap::new();
    let mut n_req = 0usize;
    for (i, m) in chosen.iter().enumerate() {
        let Some(q) = query_for(&m.title) else { continue };
        for &h in &a.horizons {
            if done.contains(&(m.ticker.clone(), h)) {
                continue;
            }
            let at = m.close_ts - h;
            if at <= m.open_ts {
                continue;
            }
            let key = (q.clone(), at / 21_600); // 6-hour buckets
            if !cache.contains_key(&key) {
                let (lo, hi) = (at - 2 * 86_400, at);
                let tone = match timeline(&client, &q, lo, hi, "timelinetone").await {
                    Ok(t) => t,
                    Err(e) => {
                        warn!(ticker = %m.ticker, error = %e, "gdelt tone failed");
                        tokio::time::sleep(Duration::from_millis(a.pace_ms * 4)).await;
                        continue;
                    }
                };
                tokio::time::sleep(Duration::from_millis(a.pace_ms)).await;
                let vol = timeline(&client, &q, lo, hi, "timelinevolraw").await.unwrap_or_default();
                tokio::time::sleep(Duration::from_millis(a.pace_ms)).await;
                n_req += 2;
                let feat = match (mean_in(&tone, at - 86_400, at), mean_in(&tone, at - 2 * 86_400, at - 86_400)) {
                    (Some(t1), Some(t0)) => Some((t1, t0, mean_in(&vol, at - 86_400, at).unwrap_or(0.0), mean_in(&vol, at - 2 * 86_400, at - 86_400).unwrap_or(0.0))),
                    _ => None,
                };
                cache.insert(key.clone(), feat);
            }
            if let Some(Some((t1, t0, v1, v0))) = cache.get(&key) {
                rows.push(NewsRow {
                    ticker: m.ticker.clone(),
                    horizon_secs: h,
                    query: q.clone(),
                    tone: *t1,
                    tone_prev: *t0,
                    articles: *v1,
                    articles_prev: *v0,
                });
            }
        }
        if i % 25 == 0 {
            write_parquet(&out_path, &rows)?;
            info!(done = i + 1, of = chosen.len(), rows = rows.len(), requests = n_req, cached = cache.len(), "news progress");
        }
    }
    write_parquet(&out_path, &rows)?;
    info!(rows = rows.len(), path = %out_path.display(), "news features written");
    Ok(())
}
