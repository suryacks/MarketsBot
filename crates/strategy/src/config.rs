use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Parameters for the short-dated crypto fair-value strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Btc15mConfig {
    /// Kalshi series to trade (e.g. KXBTC15M, KXETH15M).
    pub series: String,
    /// Reference-price symbol from the ref feed (Coinbase product id).
    pub ref_symbol: String,
    /// Minimum edge in probability points (after fees) before we take. 0.03 = 3¢.
    pub min_edge: f64,
    /// Fraction of full Kelly to size with.
    pub kelly_fraction: f64,
    /// Hard cap on |net YES contracts| per market.
    pub max_contracts_per_market: f64,
    /// Hard cap on dollars at risk per market (cost basis).
    pub max_notional_per_market: f64,
    /// Max contracts in a single order.
    pub max_order_qty: f64,
    /// Min contracts in a single taker order. Kalshi rounds each order's fee up to the
    /// cent, so 1-lots pay several times the nominal rate.
    pub min_order_qty: f64,
    /// Stop trading this many seconds before close (settlement window is the last 60s).
    pub no_trade_last_secs: i64,
    /// Don't trade in the first N seconds after open (book is still forming).
    pub warmup_secs: i64,
    /// EWMA decay per observation for realized variance.
    pub vol_lambda: f64,
    /// Take one variance sample every N seconds (tick-frequency sampling is biased upward).
    pub vol_sample_secs: f64,
    /// Floor / cap on annualized volatility used for pricing.
    pub vol_floor_annual: f64,
    pub vol_cap_annual: f64,
    /// Minimum time between orders in the same market (ms).
    pub requote_ms: i64,
    /// Settlement price is the mean of the last N seconds of the index.
    pub settle_avg_secs: f64,
    /// Only take when the book has at least this many contracts at the touch.
    pub min_touch_qty: f64,
    /// If true, also *unwind* when fair value moves against an open position by > min_edge.
    pub allow_unwind: bool,
    /// Only trade when at least this many seconds remain (the model is weakest near expiry).
    pub min_tau_secs: i64,
    /// Learn the ref-feed vs settlement-index basis from each market's strike (see basis.rs).
    pub auto_basis: bool,
    /// Fixed basis (dollars added to spot) used when auto_basis is off or has no samples yet.
    pub ref_basis: f64,
    /// EWMA weight on the previous basis estimate.
    pub basis_lambda: f64,
    /// Maker mode: rest post-only quotes at fair ∓ min_edge instead of taking the touch.
    pub maker: bool,
    /// Contracts per resting quote in maker mode.
    pub maker_qty: f64,
}

impl Default for Btc15mConfig {
    fn default() -> Self {
        Self {
            series: "KXBTC15M".into(),
            ref_symbol: "BTC-USD".into(),
            min_edge: 0.03,
            kelly_fraction: 0.25,
            max_contracts_per_market: 200.0,
            max_notional_per_market: 100.0,
            max_order_qty: 50.0,
            min_order_qty: 1.0,
            no_trade_last_secs: 75,
            warmup_secs: 5,
            vol_lambda: 0.97,
            vol_sample_secs: 60.0,
            vol_floor_annual: 0.08,
            vol_cap_annual: 2.0,
            requote_ms: 1_000,
            settle_avg_secs: 60.0,
            min_touch_qty: 1.0,
            allow_unwind: false,
            min_tau_secs: 300,
            auto_basis: true,
            ref_basis: 0.0,
            basis_lambda: 0.9,
            maker: false,
            maker_qty: 10.0,
        }
    }
}

impl Btc15mConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let s = std::fs::read_to_string(path.as_ref()).with_context(|| format!("reading {}", path.as_ref().display()))?;
        toml::from_str(&s).context("parsing strategy config")
    }
}
