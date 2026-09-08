mod cmd_backtest;
mod cmd_calibrate;
mod cmd_dashboard;
mod cmd_dataset;
mod cmd_universe;
mod cmd_history;
mod cmd_lab;
mod cmd_live;
mod cmd_news;
mod cmd_scan;
mod cmd_tools;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "mbot", version, about = "Prediction-market trading bot: data, backtests, paper & live trading")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Download Kalshi settled markets + full trade tape for a series, and Coinbase 1-min candles, into Parquet.
    FetchHistory(cmd_history::Args),
    /// Replay recorded data through a strategy with simulated fills and print a PnL report.
    Backtest(cmd_backtest::Args),
    /// Model-vs-market calibration: Brier scores and calibration tables, no trading.
    Calibrate(cmd_calibrate::Args),
    /// Exchange-wide bias scan: where does price ≠ realized frequency after fees, across many series?
    ScanBias(cmd_scan::Args),
    /// Strategy lab: run the strategies/lab.toml matrix of backtests (fetching data as needed); results feed the dashboard.
    Lab(cmd_lab::Args),
    /// Build the wide historical dataset (every liquid series, settled markets, full price paths).
    BuildDataset(cmd_dataset::Args),
    /// Evaluate thousands of parametrized strategies on the dataset with walk-forward validation.
    Universe(cmd_universe::Args),
    /// Build news sentiment features (GDELT tone/volume) for news-driven markets in the dataset.
    BuildNews(cmd_news::Args),
    /// Record live market data (Kalshi books/trades, Coinbase ref prices, Polymarket books) to Parquet.
    Collect(cmd_live::CollectArgs),
    /// Paper-trade a strategy on live data with simulated fills (no orders sent).
    Paper(cmd_live::PaperArgs),
    /// Trade for real on Kalshi with hard risk caps (demo env by default; prod needs an explicit flag).
    Live(cmd_live::LiveArgs),
    /// List open markets in a Kalshi series.
    Markets(cmd_tools::MarketsArgs),
    /// Show the live orderbook for a Kalshi ticker (needs API keys) or Polymarket token id.
    Book(cmd_tools::BookArgs),
    /// Scan Polymarket multi-outcome events and Kalshi/Polymarket pairs for structural arbitrage.
    ArbScan(cmd_tools::ArbScanArgs),
    /// Show Kalshi account balance/positions (needs API keys).
    Account,
    /// Local web dashboard: live runs, positions, fills, backtests, data, logs (http://localhost:8080).
    Dashboard(cmd_dashboard::Args),
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with_target(false)
        .compact()
        .init();

    match Cli::parse().cmd {
        Cmd::FetchHistory(a) => cmd_history::run(a).await,
        Cmd::Backtest(a) => cmd_backtest::run(a).await,
        Cmd::Calibrate(a) => cmd_calibrate::run(a).await,
        Cmd::ScanBias(a) => cmd_scan::run(a).await,
        Cmd::Lab(a) => cmd_lab::run(a).await,
        Cmd::BuildDataset(a) => cmd_dataset::run(a).await,
        Cmd::Universe(a) => cmd_universe::run(a).await,
        Cmd::BuildNews(a) => cmd_news::run(a).await,
        Cmd::Collect(a) => cmd_live::collect(a).await,
        Cmd::Paper(a) => cmd_live::paper(a).await,
        Cmd::Live(a) => cmd_live::live(a).await,
        Cmd::Markets(a) => cmd_tools::markets(a).await,
        Cmd::Book(a) => cmd_tools::book(a).await,
        Cmd::ArbScan(a) => cmd_tools::arb_scan(a).await,
        Cmd::Account => cmd_tools::account().await,
        Cmd::Dashboard(a) => cmd_dashboard::run(a).await,
    }
}
