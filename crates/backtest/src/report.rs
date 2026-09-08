use mb_core::{Fill, Fp, Outcome};
use serde::Serialize;
use std::path::Path;

#[derive(Debug, Clone, Serialize)]
pub struct MarketPnl {
    pub ticker: String,
    pub result: String,
    pub pnl: f64,
    pub fees: f64,
    pub n_fills: u32,
    pub volume: f64,
    pub final_yes_qty: f64,
    pub close_ts_ms: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Report {
    pub strategy: String,
    pub markets_seen: usize,
    pub markets_traded: usize,
    pub n_fills: usize,
    pub contracts: f64,
    pub gross_pnl: f64,
    pub fees: f64,
    pub net_pnl: f64,
    pub win_rate: f64,
    pub avg_pnl_per_market: f64,
    pub pnl_per_contract: f64,
    pub max_drawdown: f64,
    pub sharpe_per_market: f64,
    /// mean per-market PnL / standard error — the honest "is this real" number.
    pub t_stat: f64,
    pub initial_cash: f64,
    pub final_cash: f64,
    pub markets: Vec<MarketPnl>,
}

impl Report {
    pub fn build(strategy: &str, initial_cash: Fp, final_cash: Fp, markets_seen: usize, settled: &[(String, Outcome, mb_core::Position, Fp)], fills: &[Fill], close_ts: &std::collections::HashMap<String, i64>) -> Self {
        let mut markets: Vec<MarketPnl> = settled
            .iter()
            .filter(|(_, _, p, _)| p.n_fills > 0)
            .map(|(t, r, p, pnl)| MarketPnl {
                ticker: t.clone(),
                result: r.as_str().into(),
                pnl: pnl.to_f64(),
                fees: p.fees.to_f64(),
                n_fills: p.n_fills,
                volume: p.volume.to_f64(),
                final_yes_qty: p.yes_qty.to_f64(),
                close_ts_ms: close_ts.get(t).copied().unwrap_or(0),
            })
            .collect();
        markets.sort_by_key(|m| m.close_ts_ms);

        let n = markets.len();
        let fees: f64 = markets.iter().map(|m| m.fees).sum();
        let net: f64 = markets.iter().map(|m| m.pnl).sum();
        let contracts: f64 = markets.iter().map(|m| m.volume).sum();
        let wins = markets.iter().filter(|m| m.pnl > 0.0).count();
        let mean = if n > 0 { net / n as f64 } else { 0.0 };
        let var = if n > 1 { markets.iter().map(|m| (m.pnl - mean).powi(2)).sum::<f64>() / (n as f64 - 1.0) } else { 0.0 };
        let sharpe = if var > 0.0 { mean / var.sqrt() } else { 0.0 };

        let mut peak = 0.0f64;
        let mut cum = 0.0f64;
        let mut mdd = 0.0f64;
        for m in &markets {
            cum += m.pnl;
            peak = peak.max(cum);
            mdd = mdd.max(peak - cum);
        }

        Report {
            strategy: strategy.into(),
            markets_seen,
            markets_traded: n,
            n_fills: fills.len(),
            contracts,
            gross_pnl: net + fees,
            fees,
            net_pnl: net,
            win_rate: if n > 0 { wins as f64 / n as f64 } else { 0.0 },
            avg_pnl_per_market: mean,
            pnl_per_contract: if contracts > 0.0 { net / contracts } else { 0.0 },
            max_drawdown: mdd,
            sharpe_per_market: sharpe,
            t_stat: sharpe * (n as f64).sqrt(),
            initial_cash: initial_cash.to_f64(),
            final_cash: final_cash.to_f64(),
            markets,
        }
    }

    pub fn summary(&self) -> String {
        format!(
            "strategy            {}\n\
             markets seen/traded {} / {}\n\
             fills / contracts   {} / {:.0}\n\
             gross pnl           ${:.2}\n\
             fees                ${:.2}\n\
             NET PNL             ${:.2}   ({:.2}% of ${:.0} bankroll)\n\
             pnl per contract    ${:.4}\n\
             avg pnl / market    ${:.3}\n\
             win rate (markets)  {:.1}%\n\
             max drawdown        ${:.2}\n\
             sharpe (per-market) {:.3}   t-stat {:.2}\n",
            self.strategy,
            self.markets_seen,
            self.markets_traded,
            self.n_fills,
            self.contracts,
            self.gross_pnl,
            self.fees,
            self.net_pnl,
            if self.initial_cash > 0.0 { 100.0 * self.net_pnl / self.initial_cash } else { 0.0 },
            self.initial_cash,
            self.pnl_per_contract,
            self.avg_pnl_per_market,
            100.0 * self.win_rate,
            self.max_drawdown,
            self.sharpe_per_market,
            self.t_stat,
        )
    }

    pub fn write_json(&self, path: impl AsRef<Path>, params: serde_json::Value) -> anyhow::Result<()> {
        let path = path.as_ref();
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        }
        let mut v = serde_json::to_value(self)?;
        v["params"] = params;
        v["created_ms"] = serde_json::json!(chrono::Utc::now().timestamp_millis());
        std::fs::write(path, serde_json::to_vec_pretty(&v)?)?;
        Ok(())
    }

    pub fn write_csv(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let path = path.as_ref();
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        }
        let mut s = String::from("ticker,result,pnl,fees,n_fills,volume,final_yes_qty,close_ts_ms\n");
        for m in &self.markets {
            s.push_str(&format!(
                "{},{},{:.4},{:.4},{},{:.2},{:.2},{}\n",
                m.ticker, m.result, m.pnl, m.fees, m.n_fills, m.volume, m.final_yes_qty, m.close_ts_ms
            ));
        }
        std::fs::write(path, s)?;
        Ok(())
    }

    pub fn write_fills_csv(path: impl AsRef<Path>, fills: &[Fill]) -> anyhow::Result<()> {
        let path = path.as_ref();
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        }
        let mut s = String::from("ts_ms,ticker,action,yes_px,qty,fee,is_maker,tag\n");
        for f in fills {
            s.push_str(&format!(
                "{},{},{:?},{},{},{},{},{}\n",
                f.ts_ms, f.ticker, f.action, f.yes_px, f.qty, f.fee, f.is_maker, f.tag
            ));
        }
        std::fs::write(path, s)?;
        Ok(())
    }
}
