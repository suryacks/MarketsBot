//! Local dashboard: serves `dashboard.html` and a few JSON endpoints over the
//! files the bot already writes (state snapshots, reports, recorded data, logs).

use anyhow::Result;
use axum::extract::{Query, State};
use axum::response::{Html, IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use clap::Args as ClapArgs;
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

#[derive(ClapArgs, Debug, Clone)]
pub struct Args {
    #[arg(long, default_value_t = 8080)]
    pub port: u16,
    #[arg(long, default_value = "data/state")]
    pub state_dir: PathBuf,
    #[arg(long, default_value = "reports")]
    pub reports_dir: PathBuf,
    #[arg(long, default_value = "logs")]
    pub logs_dir: PathBuf,
    #[arg(long, default_value = "reports/lab")]
    pub lab_dir: PathBuf,
    /// Data roots to summarize (repeatable)
    #[arg(long = "data", default_values_t = vec!["data".to_string(), "data/live".to_string(), "data/live-weather".to_string()])]
    pub data_dirs: Vec<String>,
}

type Shared = Arc<Args>;

const HTML: &str = include_str!("../dashboard.html");

async fn index() -> Html<&'static str> {
    Html(HTML)
}

fn read_json_files(dir: &Path) -> Vec<Value> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().is_some_and(|x| x == "json")
                && let Ok(s) = std::fs::read(&p)
                && let Ok(mut v) = serde_json::from_slice::<Value>(&s)
            {
                v["_file"] = json!(p.file_name().unwrap().to_string_lossy());
                out.push(v);
            }
        }
    }
    out
}

async fn runs(State(a): State<Shared>) -> Json<Value> {
    let mut runs = read_json_files(&a.state_dir);
    runs.sort_by_key(|r| -(r["updated_ms"].as_i64().unwrap_or(0)));
    Json(json!(runs))
}

async fn lab(State(a): State<Shared>) -> Json<Value> {
    let mut v = read_json_files(&a.lab_dir);
    v.sort_by_key(|r| r["name"].as_str().unwrap_or("").to_string());
    Json(json!(v))
}

async fn reports(State(a): State<Shared>) -> Json<Value> {
    let mut rs: Vec<Value> = read_json_files(&a.reports_dir)
        .into_iter()
        .map(|mut r| {
            // keep the payload light for the list; markets are fetched per report
            let n = r["markets"].as_array().map(|m| m.len()).unwrap_or(0);
            r["n_markets_rows"] = json!(n);
            r.as_object_mut().unwrap().remove("markets");
            if r["kind"] == "bias-scan" {
                r.as_object_mut().unwrap().remove("rows");
                r.as_object_mut().unwrap().remove("series");
            }
            r
        })
        .collect();
    rs.sort_by_key(|r| -(r["created_ms"].as_i64().unwrap_or(0)));
    Json(json!(rs))
}

#[derive(Deserialize)]
struct NameQ {
    name: String,
}

async fn report(State(a): State<Shared>, Query(q): Query<NameQ>) -> impl IntoResponse {
    let safe = Path::new(&q.name).file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or_default();
    match std::fs::read(a.reports_dir.join(&safe)) {
        Ok(s) => Json(serde_json::from_slice::<Value>(&s).unwrap_or(Value::Null)),
        Err(_) => Json(Value::Null),
    }
}

fn dir_summary(root: &Path) -> Value {
    let mut kinds = serde_json::Map::new();
    if let Ok(rd) = std::fs::read_dir(root) {
        for e in rd.flatten() {
            let p = e.path();
            if !p.is_dir() {
                continue;
            }
            let mut files = 0u64;
            let mut bytes = 0u64;
            let mut newest = 0i64;
            fn walk(d: &Path, files: &mut u64, bytes: &mut u64, newest: &mut i64) {
                if let Ok(rd) = std::fs::read_dir(d) {
                    for e in rd.flatten() {
                        let p = e.path();
                        if p.is_dir() {
                            walk(&p, files, bytes, newest);
                        } else if p.extension().is_some_and(|x| x == "parquet")
                            && let Ok(m) = p.metadata()
                        {
                            *files += 1;
                            *bytes += m.len();
                            if let Ok(t) = m.modified()
                                && let Ok(d) = t.duration_since(std::time::UNIX_EPOCH)
                            {
                                *newest = (*newest).max(d.as_millis() as i64);
                            }
                        }
                    }
                }
            }
            walk(&p, &mut files, &mut bytes, &mut newest);
            kinds.insert(
                p.file_name().unwrap().to_string_lossy().to_string(),
                json!({"files": files, "bytes": bytes, "newest_ms": newest}),
            );
        }
    }
    json!({"root": root.to_string_lossy(), "kinds": kinds})
}

async fn data(State(a): State<Shared>) -> Json<Value> {
    Json(json!(a.data_dirs.iter().map(|d| dir_summary(Path::new(d))).collect::<Vec<_>>()))
}

#[derive(Deserialize)]
struct LogQ {
    file: Option<String>,
    lines: Option<usize>,
}

async fn logs(State(a): State<Shared>, Query(q): Query<LogQ>) -> Json<Value> {
    match q.file {
        None => {
            let mut files: Vec<(i64, String, u64)> = std::fs::read_dir(&a.logs_dir)
                .map(|rd| {
                    rd.flatten()
                        .filter_map(|e| {
                            let m = e.metadata().ok()?;
                            let t = m.modified().ok()?.duration_since(std::time::UNIX_EPOCH).ok()?.as_millis() as i64;
                            Some((t, e.file_name().to_string_lossy().to_string(), m.len()))
                        })
                        .collect()
                })
                .unwrap_or_default();
            files.sort_by_key(|f| -f.0);
            Json(json!(files.iter().map(|(t, n, s)| json!({"file": n, "modified_ms": t, "bytes": s})).collect::<Vec<_>>()))
        }
        Some(f) => {
            let safe = Path::new(&f).file_name().map(|x| x.to_string_lossy().to_string()).unwrap_or_default();
            let n = q.lines.unwrap_or(200).min(2000);
            let text = std::fs::read_to_string(a.logs_dir.join(&safe)).unwrap_or_default();
            let re = regex_strip_ansi(&text);
            let lines: Vec<&str> = re.lines().rev().take(n).collect();
            Json(json!({"file": safe, "lines": lines.into_iter().rev().collect::<Vec<_>>()}))
        }
    }
}

/// Strip ANSI colour codes without pulling in a regex crate.
fn regex_strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            if chars.peek() == Some(&'[') {
                chars.next();
                for d in chars.by_ref() {
                    if d.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}

pub async fn run(a: Args) -> Result<()> {
    let shared: Shared = Arc::new(a.clone());
    let app = Router::new()
        .route("/", get(index))
        .route("/api/runs", get(runs))
        .route("/api/reports", get(reports))
        .route("/api/lab", get(lab))
        .route("/api/report", get(report))
        .route("/api/data", get(data))
        .route("/api/logs", get(logs))
        .with_state(shared);
    let addr = format!("127.0.0.1:{}", a.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("dashboard at http://{addr}  (state {}, reports {}, logs {})", a.state_dir.display(), a.reports_dir.display(), a.logs_dir.display());
    axum::serve(listener, app).await?;
    Ok(())
}
