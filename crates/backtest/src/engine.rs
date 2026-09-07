use crate::report::Report;
use crate::sim::SimExchange;
use mb_core::{Fill, MarketEvent, Strategy};
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
