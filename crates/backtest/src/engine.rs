use crate::report::Report;
use crate::sim::SimExchange;
use mb_core::{Context as _, Fill, MarketEvent, Strategy};
use std::collections::HashMap;
use tracing::info;

pub struct Backtester {
    pub sim: SimExchange,
    pub strategy: Box<dyn Strategy>,
    fills: Vec<Fill>,
    close_ts: HashMap<String, i64>,
    markets_seen: usize,
}

impl Backtester {
    pub fn new(sim: SimExchange, strategy: Box<dyn Strategy>) -> Self {
        Self {
            sim,
            strategy,
            fills: Vec::new(),
            close_ts: HashMap::new(),
            markets_seen: 0,
        }
    }

    /// Feed one event through the simulator and the strategy.
    pub fn step(&mut self, ev: &MarketEvent) {
        if let MarketEvent::Market(m) = ev {
            if !self.close_ts.contains_key(&m.ticker) {
                self.markets_seen += 1;
            }
            self.close_ts.insert(m.ticker.clone(), m.close_ts_ms);
        }
        self.sim.on_event(ev);
        self.strategy.on_event(ev, &mut self.sim);
        for f in self.sim.drain_fills() {
            self.strategy.on_fill(&f, &mut self.sim);
            self.fills.push(f);
        }
        if let MarketEvent::Settlement { ticker, result, .. } = ev {
            self.sim.settle(ticker, *result);
        }
    }

    pub fn run(&mut self, events: &[MarketEvent]) {
        let n = events.len();
        let t0 = std::time::Instant::now();
        for (i, ev) in events.iter().enumerate() {
            self.step(ev);
            if i % 500_000 == 0 && i > 0 {
                info!(progress = format!("{}/{}", i, n), cash = %self.sim.total_cash(), "backtest");
            }
        }
        let dt = t0.elapsed();
        info!(events = n, secs = format!("{:.2}", dt.as_secs_f64()), rate = format!("{:.0}/s", n as f64 / dt.as_secs_f64().max(1e-9)), "backtest done");
    }

    pub fn fills(&self) -> &[Fill] {
        &self.fills
    }

    /// Dashboard snapshot of everything the engine knows.
    pub fn state(&self, run_id: &str, mode: &str, initial_cash: mb_core::Fp, started_ms: i64) -> serde_json::Value {
        use serde_json::json;
        let now = chrono::Utc::now().timestamp_millis();
        let positions: Vec<serde_json::Value> = self
            .sim
            .positions()
            .values()
            .filter(|p| !p.yes_qty.is_zero() || p.n_fills > 0)
            .map(|p| {
                let bk = self.sim.book(&p.ticker);
                let mid = bk.and_then(|b| b.mid());
                let (mark, mtm, value) = mb_core::mark_position(p, mid, bk.and_then(|b| b.best_bid()).map(|(x, _)| x), bk.and_then(|b| b.best_ask()).map(|(x, _)| x));
                json!({
                    "ticker": p.ticker, "yes_qty": p.yes_qty.to_f64(), "cash": p.cash.to_f64(),
                    "fees": p.fees.to_f64(), "n_fills": p.n_fills, "volume": p.volume.to_f64(),
                    "mid": mid.map(|m| m.to_f64()), "mark": mark, "mtm": mtm, "value": value,
                    "close_ts_ms": self.close_ts.get(&p.ticker),
                })
            })
            .collect();
        let unrealized: f64 = positions.iter().map(|p| p["mtm"].as_f64().unwrap_or(0.0)).sum();
        // Equity is cash plus what the open positions are WORTH, not cash plus their P&L:
        // the purchase already left `cash`, so adding P&L would deduct it a second time.
        let pos_value: f64 = positions.iter().map(|p| p["value"].as_f64().unwrap_or(0.0)).sum();
        let settled: Vec<serde_json::Value> = self
            .sim
            .settled
            .iter()
            .map(|(t, r, p, pnl)| {
                json!({"ticker": t, "result": r.as_str(), "pnl": pnl.to_f64(), "fees": p.fees.to_f64(),
                       "n_fills": p.n_fills, "volume": p.volume.to_f64(), "close_ts_ms": self.close_ts.get(t)})
            })
            .collect();
        let settled_pnl: f64 = settled.iter().map(|s| s["pnl"].as_f64().unwrap_or(0.0)).sum();
        let fills: Vec<serde_json::Value> = self
            .fills
            .iter()
            .rev()
            .take(200)
            .map(|f| {
                json!({"ts_ms": f.ts_ms, "ticker": f.ticker, "action": format!("{:?}", f.action), "yes_px": f.yes_px.to_f64(),
                       "qty": f.qty.to_f64(), "fee": f.fee.to_f64(), "is_maker": f.is_maker, "tag": f.tag})
            })
            .collect();
        let open_orders: Vec<serde_json::Value> = self
            .sim
            .resting_orders()
            .iter()
            .map(|(id, r, rem, ahead)| {
                json!({"id": id.0, "ticker": r.ticker, "action": format!("{:?}", r.action), "yes_px": r.yes_px.to_f64(),
                       "qty": r.qty.to_f64(), "remaining": rem.to_f64(), "ahead": ahead.to_f64(), "tag": r.tag})
            })
            .collect();
        let books: serde_json::Map<String, serde_json::Value> = self
            .sim
            .books()
            .iter()
            .map(|(t, b)| {
                (
                    t.clone(),
                    json!({"bid": b.best_bid().map(|(p, q)| [p.to_f64(), q.to_f64()]), "ask": b.best_ask().map(|(p, q)| [p.to_f64(), q.to_f64()]),
                           "mid": b.mid().map(|m| m.to_f64()), "ts_ms": b.ts_ms}),
                )
            })
            .collect();
        let qs = &self.sim.queue_stats;
        let wins = settled.iter().filter(|s| s["pnl"].as_f64().unwrap_or(0.0) > 0.0).count();
        json!({
            "run_id": run_id, "mode": mode, "strategy": self.strategy.name(),
            "started_ms": started_ms, "updated_ms": now,
            "initial_cash": initial_cash.to_f64(), "cash": self.sim.total_cash().to_f64(), "free_cash": self.sim.free_cash().to_f64(),
            "settled_pnl": settled_pnl, "unrealized": unrealized,
            "equity": self.sim.total_cash().to_f64() + pos_value,
            "n_fills": self.fills.len(), "markets_seen": self.markets_seen,
            "settled_count": settled.len(), "settled_wins": wins,
            "positions": positions, "settled": settled, "fills": fills, "open_orders": open_orders, "books": books,
            "queue": {"rested": qs.orders_rested, "avg_ahead": if qs.orders_rested > 0 { qs.ahead_at_insert / qs.orders_rested as f64 } else { 0.0 },
                      "reached_front": qs.reached_front, "fills_at_price": qs.fills_at_price, "fills_through": qs.fills_through},
            "strategy_state": self.strategy.snapshot(),
        })
    }

    /// Atomically write `state()` to `path` (tmp + rename).
    pub fn write_state(path: &std::path::Path, state: &serde_json::Value) -> anyhow::Result<()> {
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p)?;
        }
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec(state)?)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    pub fn report(&self, initial_cash: mb_core::Fp) -> Report {
        Report::build(
            self.strategy.name(),
            initial_cash,
            self.sim.total_cash(),
            self.markets_seen,
            &self.sim.settled,
            &self.fills,
            &self.close_ts,
        )
    }
}
