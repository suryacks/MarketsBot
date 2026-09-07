//! Short-dated crypto binary fair-value strategy.
//!
//! Signal: model probability from spot + realized vol vs. the Kalshi touch.
//! Execution: IOC limit orders against the touch when |fair − price| exceeds
//! fees + `min_edge`, sized by fractional Kelly with hard caps. Positions are
//! held to settlement (15-minute contracts).

use crate::config::Btc15mConfig;
use crate::fair_value::{kelly_fraction_buy, prob_above};
use crate::vol::RealizedVol;
use mb_core::{Action, Context, Fill, Fp, MarketEvent, MarketInfo, OrderRequest, Strategy, Tif};
use std::collections::HashMap;
use tracing::{debug, info};

#[derive(Debug, Clone)]
struct Active {
    strike: f64,
    open_ts_ms: i64,
    close_ts_ms: i64,
    last_order_ts_ms: i64,
    /// Sum of cost basis in dollars spent in this market.
    notional: f64,
}

pub struct Btc15mStrategy {
    cfg: Btc15mConfig,
    vol: RealizedVol,
    spot: Option<(i64, f64)>,
    active: HashMap<String, Active>,
    pub stats: Stats,
}

#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub evaluations: u64,
    pub signals: u64,
    pub orders: u64,
    pub skipped_no_book: u64,
    pub skipped_window: u64,
}

impl Btc15mStrategy {
    pub fn new(cfg: Btc15mConfig) -> Self {
        let vol = RealizedVol::new(
            cfg.vol_lambda,
            cfg.vol_floor_annual,
            cfg.vol_cap_annual,
            (cfg.vol_sample_secs * 1000.0) as i64,
        );
        Self {
            cfg,
            vol,
            spot: None,
            active: HashMap::new(),
            stats: Stats::default(),
        }
    }

    pub fn config(&self) -> &Btc15mConfig {
        &self.cfg
    }

    pub fn sigma_annual(&self) -> f64 {
        self.vol.sigma_annual()
    }

    fn on_market(&mut self, m: &MarketInfo) {
        if m.series != self.cfg.series {
            return;
        }
        let Some(k) = m.floor_strike else { return };
        if m.close_ts_ms <= 0 {
            return;
        }
        let entry = self.active.entry(m.ticker.clone()).or_insert(Active {
            strike: k,
            open_ts_ms: m.open_ts_ms,
            close_ts_ms: m.close_ts_ms,
            last_order_ts_ms: 0,
            notional: 0.0,
        });
        entry.strike = k;
        entry.close_ts_ms = m.close_ts_ms;
        entry.open_ts_ms = m.open_ts_ms;
        debug!(ticker = %m.ticker, strike = k, "tracking market");
    }

    /// Fair probability of YES for a tracked market right now.
    pub fn fair(&self, ticker: &str, now_ms: i64) -> Option<f64> {
        let a = self.active.get(ticker)?;
        let (_, spot) = self.spot?;
        let tau = (a.close_ts_ms - now_ms) as f64 / 1000.0;
        Some(prob_above(spot, a.strike, self.vol.sigma_per_sec(), tau, self.cfg.settle_avg_secs))
    }

    fn evaluate(&mut self, ticker: &str, ctx: &mut dyn Context) {
        let now = ctx.now_ms();
        let Some(a) = self.active.get(ticker).cloned() else { return };
        let Some((spot_ts, spot)) = self.spot else { return };
        self.stats.evaluations += 1;

        // trading window
        let secs_left = (a.close_ts_ms - now) / 1000;
        if secs_left < self.cfg.no_trade_last_secs || now < a.open_ts_ms + self.cfg.warmup_secs * 1000 {
            self.stats.skipped_window += 1;
            return;
        }
        if now - a.last_order_ts_ms < self.cfg.requote_ms {
            return;
        }
        // stale reference price guard (>10s old)
        if now - spot_ts > 10_000 {
            return;
        }

        let Some(book) = ctx.book(ticker) else {
            self.stats.skipped_no_book += 1;
            return;
        };
        let (best_bid, best_ask) = (book.best_bid(), book.best_ask());

        let tau = (a.close_ts_ms - now) as f64 / 1000.0;
        let fair = prob_above(spot, a.strike, self.vol.sigma_per_sec(), tau, self.cfg.settle_avg_secs);
        let pos = ctx.position(ticker);
        let fee_model = ctx.fee_model(ticker);
        let bankroll = ctx.cash().to_f64().max(0.0);
        let max_q = self.cfg.max_contracts_per_market;
        let min_touch = Fp::from_f64(self.cfg.min_touch_qty);

        // --- buy YES if fair > ask + fee + edge ---
        if let Some((ask, ask_qty)) = best_ask
            && ask_qty >= min_touch
        {
            let a_px = ask.to_f64();
            let fee = fee_model.fee_per_contract(ask, false);
            let edge = fair - a_px - fee;
            let room = max_q - pos.yes_qty.to_f64();
            let notional_room = self.cfg.max_notional_per_market - a.notional;
            if edge > self.cfg.min_edge && room > 0.0 && notional_room > 0.0 {
                let f = kelly_fraction_buy(fair, a_px + fee) * self.cfg.kelly_fraction;
                let qty = (f * bankroll / a_px)
                    .min(room)
                    .min(self.cfg.max_order_qty)
                    .min(notional_room / a_px)
                    .min(ask_qty.to_f64())
                    .floor();
                if qty >= 1.0 {
                    self.stats.signals += 1;
                    let req = OrderRequest::buy_yes(ticker, ask, Fp::from_int(qty as i64), Tif::Ioc).tagged("fv_buy_yes");
                    info!(ticker, fair = format!("{fair:.3}"), ask = %ask, edge = format!("{edge:.3}"), qty, "BUY YES");
                    ctx.submit(req);
                    self.stats.orders += 1;
                    if let Some(x) = self.active.get_mut(ticker) {
                        x.last_order_ts_ms = now;
                    }
                    return;
                }
            }
        }

        // --- sell YES (buy NO) if bid > fair + fee + edge ---
        if let Some((bid, bid_qty)) = best_bid
            && bid_qty >= min_touch
        {
            let b_px = bid.to_f64();
            let fee = fee_model.fee_per_contract(bid, false);
            let edge = b_px - fair - fee;
            let room = max_q + pos.yes_qty.to_f64(); // how much more we can go short
            let no_px = 1.0 - b_px;
            let notional_room = self.cfg.max_notional_per_market - a.notional;
            if edge > self.cfg.min_edge && room > 0.0 && notional_room > 0.0 {
                let f = kelly_fraction_buy(1.0 - fair, no_px + fee) * self.cfg.kelly_fraction;
                let qty = (f * bankroll / no_px)
                    .min(room)
                    .min(self.cfg.max_order_qty)
                    .min(notional_room / no_px)
                    .min(bid_qty.to_f64())
                    .floor();
                if qty >= 1.0 {
                    self.stats.signals += 1;
                    let req = OrderRequest::sell_yes(ticker, bid, Fp::from_int(qty as i64), Tif::Ioc).tagged("fv_buy_no");
                    info!(ticker, fair = format!("{fair:.3}"), bid = %bid, edge = format!("{edge:.3}"), qty, "BUY NO");
                    ctx.submit(req);
                    self.stats.orders += 1;
                    if let Some(x) = self.active.get_mut(ticker) {
                        x.last_order_ts_ms = now;
                    }
                }
            }
        }
    }
}

impl Strategy for Btc15mStrategy {
    fn name(&self) -> &str {
        "btc15m_fair_value"
    }

    fn on_event(&mut self, ev: &MarketEvent, ctx: &mut dyn Context) {
        match ev {
            MarketEvent::Market(m) => self.on_market(m),
            MarketEvent::Ref(r) => {
                if r.symbol != self.cfg.ref_symbol {
                    return;
                }
                self.vol.update(r.ts_ms, r.px);
                self.spot = Some((r.ts_ms, r.px));
                let tickers: Vec<String> = self.active.keys().cloned().collect();
                for t in tickers {
                    self.evaluate(&t, ctx);
                }
            }
            MarketEvent::BookSnapshot { ticker, .. }
            | MarketEvent::BookDelta { ticker, .. }
            | MarketEvent::BookLevel { ticker, .. }
            | MarketEvent::Ticker { ticker, .. } => {
                if self.active.contains_key(ticker) {
                    let t = ticker.clone();
                    self.evaluate(&t, ctx);
                }
            }
            MarketEvent::Trade(t) => {
                if self.active.contains_key(&t.ticker) {
                    let tk = t.ticker.clone();
                    self.evaluate(&tk, ctx);
                }
            }
            MarketEvent::Settlement { ticker, .. } => {
                self.active.remove(ticker);
            }
        }
    }

    fn on_fill(&mut self, fill: &Fill, _ctx: &mut dyn Context) {
        if let Some(a) = self.active.get_mut(&fill.ticker) {
            let cost = match fill.action {
                Action::Buy => fill.yes_px.mul(fill.qty),
                Action::Sell => fill.yes_px.complement().mul(fill.qty),
            };
            a.notional += cost.to_f64() + fill.fee.to_f64();
        }
    }
}
