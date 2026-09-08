//! Short-dated crypto binary fair-value strategy.
//!
//! Signal: model probability from (spot + basis) and realized vol vs. the
//! Kalshi touch. Two execution styles:
//! * **taker** – IOC against the touch when |fair − price| > fee + min_edge,
//!   sized by fractional Kelly with hard caps;
//! * **maker** – rest post-only quotes at fair ∓ min_edge (zero maker fee on
//!   most Kalshi series), re-quoted when fair moves.
//! Positions are held to settlement (15-minute contracts).

use crate::basis::BasisEstimator;
use crate::config::Btc15mConfig;
use crate::fair_value::{kelly_fraction_buy, prob_above, prob_above_endgame};
use crate::vol::{ImpliedVol, RealizedVol, VolModel, VolSource};
use mb_core::{Action, Context, Fill, Fp, MarketEvent, MarketInfo, OrderId, OrderRequest, Strategy, Tif};
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
    entries: u32,
    bid_order: Option<(OrderId, Fp)>,
    ask_order: Option<(OrderId, Fp)>,
}

pub struct Btc15mStrategy {
    cfg: Btc15mConfig,
    vol: VolModel,
    basis: BasisEstimator,
    spot: Option<(i64, f64)>,
    active: HashMap<String, Active>,
    /// Recent basis-adjusted ref prices (ts_ms, px) for the running settlement average.
    recent: std::collections::VecDeque<(i64, f64)>,
    /// Official running 60 s average from the index feed, if the feed provides it.
    official_avg: Option<(i64, f64)>,
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

/// Kalshi's tapered price grid: 0.001 below $0.10 and above $0.90, else $0.01.
pub fn kalshi_tick(px: Fp) -> Fp {
    if px < Fp::parse("0.10").unwrap() || px > Fp::parse("0.90").unwrap() {
        Fp::TICK
    } else {
        Fp::CENT
    }
}

impl Btc15mStrategy {
    pub fn new(cfg: Btc15mConfig) -> Self {
        let vol = VolModel::new(
            RealizedVol::new(cfg.vol_lambda, cfg.vol_floor_annual, cfg.vol_cap_annual, (cfg.vol_sample_secs * 1000.0) as i64),
            ImpliedVol::new(cfg.iv_lambda),
            VolSource::parse(&cfg.vol_source),
            cfg.vol_floor_annual,
            cfg.vol_cap_annual,
        );
        let basis = BasisEstimator::new(if cfg.basis_window_secs > 0.0 { cfg.basis_window_secs } else { cfg.settle_avg_secs.max(60.0) }, cfg.basis_lambda);
        Self {
            cfg,
            vol,
            basis,
            spot: None,
            active: HashMap::new(),
            recent: std::collections::VecDeque::new(),
            official_avg: None,
            stats: Stats::default(),
        }
    }

    /// Running settlement average: the feed's official 60 s average when fresh (≤ 3 s), else
    /// the mean of the basis-adjusted ref price over [close − window, now] (≥ 2 points).
    fn running_avg(&self, close_ts_ms: i64, window_secs: f64) -> Option<f64> {
        if let Some((t, a)) = self.official_avg
            && let Some((now, _)) = self.spot
            && now - t <= 3_000
        {
            return Some(a);
        }
        let start = close_ts_ms - (window_secs * 1000.0) as i64;
        let b = self.effective_basis();
        let pts: Vec<f64> = self.recent.iter().filter(|(t, _)| *t >= start).map(|(_, p)| p + b).collect();
        if pts.len() >= 2 { Some(pts.iter().sum::<f64>() / pts.len() as f64) } else { None }
    }

    pub fn config(&self) -> &Btc15mConfig {
        &self.cfg
    }
    pub fn sigma_annual(&self) -> f64 {
        self.vol.sigma_annual()
    }
    pub fn basis(&self) -> f64 {
        self.effective_basis()
    }

    fn effective_basis(&self) -> f64 {
        if self.cfg.auto_basis && self.basis.samples() > 0 {
            self.basis.basis()
        } else {
            self.cfg.ref_basis
        }
    }

    fn on_market(&mut self, m: &MarketInfo) {
        if m.series != self.cfg.series {
            return;
        }
        let Some(k) = m.floor_strike else { return };
        if m.close_ts_ms <= 0 {
            return;
        }
        if !self.active.contains_key(&m.ticker) {
            if let Some(s) = self.basis.on_market_open(m.open_ts_ms, k) {
                debug!(ticker = %m.ticker, sample = format!("{s:.2}"), basis = format!("{:.2}", self.basis.basis()), "basis");
            }
        }
        let entry = self.active.entry(m.ticker.clone()).or_insert(Active {
            strike: k,
            open_ts_ms: m.open_ts_ms,
            close_ts_ms: m.close_ts_ms,
            last_order_ts_ms: 0,
            notional: 0.0,
            entries: 0,
            bid_order: None,
            ask_order: None,
        });
        entry.strike = k;
        entry.close_ts_ms = m.close_ts_ms;
        entry.open_ts_ms = m.open_ts_ms;
    }

    /// Fair probability of YES for a tracked market right now.
    pub fn fair(&self, ticker: &str, now_ms: i64) -> Option<f64> {
        let a = self.active.get(ticker)?;
        let (_, spot) = self.spot?;
        let tau = (a.close_ts_ms - now_ms) as f64 / 1000.0;
        Some(prob_above(spot + self.effective_basis(), a.strike, self.vol.sigma_per_sec(), tau, self.cfg.settle_avg_secs))
    }

    fn cancel_quotes(&mut self, ticker: &str, ctx: &mut dyn Context) {
        if let Some(a) = self.active.get_mut(ticker) {
            if let Some((id, _)) = a.bid_order.take() {
                ctx.cancel(id);
            }
            if let Some((id, _)) = a.ask_order.take() {
                ctx.cancel(id);
            }
        }
    }

    fn evaluate(&mut self, ticker: &str, ctx: &mut dyn Context) {
        let now = ctx.now_ms();
        let Some(a) = self.active.get(ticker).cloned() else { return };
        let Some((spot_ts, spot)) = self.spot else { return };
        self.stats.evaluations += 1;

        if self.vol.realized.samples() < self.cfg.vol_min_samples {
            self.stats.skipped_window += 1;
            return; // volatility estimate not yet meaningful
        }
        let secs_left = (a.close_ts_ms - now) / 1000;
        let tau = (a.close_ts_ms - now) as f64 / 1000.0;
        let endgame_now = self.cfg.endgame && tau < self.cfg.settle_avg_secs && tau >= self.cfg.endgame_stop_secs;
        let in_window = if endgame_now {
            now - spot_ts <= 3_000
        } else {
            secs_left >= self.cfg.no_trade_last_secs.max(self.cfg.min_tau_secs)
                && now >= a.open_ts_ms + self.cfg.warmup_secs * 1000
                && now - spot_ts <= 10_000
        };
        if !in_window {
            self.stats.skipped_window += 1;
            if self.cfg.maker {
                self.cancel_quotes(ticker, ctx);
            }
            return;
        }

        let model = if endgame_now {
            match self.running_avg(a.close_ts_ms, self.cfg.settle_avg_secs) {
                Some(avg) => prob_above_endgame(spot + self.effective_basis(), a.strike, self.vol.sigma_per_sec(), tau, self.cfg.settle_avg_secs, avg),
                None => return,
            }
        } else {
            prob_above(spot + self.effective_basis(), a.strike, self.vol.sigma_per_sec(), tau, self.cfg.settle_avg_secs)
        };
        let fair = match (self.cfg.market_blend > 0.0, ctx.book(ticker).and_then(|b| b.mid())) {
            (true, Some(mid)) => (1.0 - self.cfg.market_blend) * model + self.cfg.market_blend * mid.to_f64(),
            _ => model,
        };
        if fair < self.cfg.fair_min || fair > self.cfg.fair_max {
            self.stats.skipped_window += 1;
            if self.cfg.maker {
                self.cancel_quotes(ticker, ctx);
            }
            return;
        }

        // Inversion is applied to the fair value itself, so every downstream decision
        // (side, Kelly size, edge test) flips consistently.
        let fair = if self.cfg.invert { 1.0 - fair } else { fair };
        if self.cfg.maker && !endgame_now {
            self.evaluate_maker(ticker, fair, ctx);
        } else {
            let requote = if endgame_now { 250 } else { self.cfg.requote_ms };
            if now - a.last_order_ts_ms < requote {
                return;
            }
            self.evaluate_taker(ticker, fair, &a, ctx);
        }
    }

    fn evaluate_taker(&mut self, ticker: &str, fair: f64, a: &Active, ctx: &mut dyn Context) {
        let now = ctx.now_ms();
        let Some(book) = ctx.book(ticker) else {
            self.stats.skipped_no_book += 1;
            return;
        };
        let (best_bid, best_ask) = (book.best_bid(), book.best_ask());
        if a.entries >= self.cfg.max_entries_per_market {
            return;
        }
        let pos = ctx.position(ticker);
        let fee_model = ctx.fee_model(ticker);
        let bankroll = ctx.cash().to_f64().max(0.0);
        let max_q = self.cfg.max_contracts_per_market;
        let min_touch = Fp::from_f64(self.cfg.min_touch_qty);
        let notional_cap = self.cfg.max_notional_per_market.min(self.cfg.notional_frac_of_cash * bankroll);
        let notional_room = notional_cap - a.notional;

        // --- buy YES if fair > ask + fee + edge ---
        if let Some((ask, ask_qty)) = best_ask
            && ask_qty >= min_touch
        {
            let a_px = ask.to_f64();
            let fee = fee_model.fee_per_contract(ask, false);
            let edge = fair - a_px - fee;
            let room = max_q - pos.yes_qty.to_f64();
            if edge > self.cfg.min_edge && room > 0.0 && notional_room > 0.0 {
                let f = kelly_fraction_buy(fair, a_px + fee) * self.cfg.kelly_fraction;
                let qty = (f * bankroll / a_px)
                    .min(room)
                    .min(self.cfg.max_order_qty)
                    .min(notional_room / a_px)
                    .min(ask_qty.to_f64())
                    .floor();
                if qty >= self.cfg.min_order_qty.max(1.0) {
                    self.stats.signals += 1;
                    let req = OrderRequest::buy_yes(ticker, ask, Fp::from_int(qty as i64), Tif::Ioc).tagged("fv_buy_yes");
                    info!(ticker, fair = format!("{fair:.3}"), ask = %ask, edge = format!("{edge:.3}"), qty, "BUY YES");
                    ctx.submit(req);
                    self.stats.orders += 1;
                    if let Some(x) = self.active.get_mut(ticker) {
                        x.last_order_ts_ms = now;
                        x.entries += 1;
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
            let room = max_q + pos.yes_qty.to_f64();
            let no_px = 1.0 - b_px;
            if edge > self.cfg.min_edge && room > 0.0 && notional_room > 0.0 {
                let f = kelly_fraction_buy(1.0 - fair, no_px + fee) * self.cfg.kelly_fraction;
                let qty = (f * bankroll / no_px)
                    .min(room)
                    .min(self.cfg.max_order_qty)
                    .min(notional_room / no_px)
                    .min(bid_qty.to_f64())
                    .floor();
                if qty >= self.cfg.min_order_qty.max(1.0) {
                    self.stats.signals += 1;
                    let req = OrderRequest::sell_yes(ticker, bid, Fp::from_int(qty as i64), Tif::Ioc).tagged("fv_buy_no");
                    info!(ticker, fair = format!("{fair:.3}"), bid = %bid, edge = format!("{edge:.3}"), qty, "BUY NO");
                    ctx.submit(req);
                    self.stats.orders += 1;
                    if let Some(x) = self.active.get_mut(ticker) {
                        x.last_order_ts_ms = now;
                        x.entries += 1;
                    }
                }
            }
        }
    }

    /// Rest a bid at fair − edge and an ask at fair + edge (post-only, never crossing
    /// the touch), cancel/replace when the desired price changes.
    fn evaluate_maker(&mut self, ticker: &str, fair: f64, ctx: &mut dyn Context) {
        let pos = ctx.position(ticker);
        let max_q = self.cfg.max_contracts_per_market;
        let (touch_bid, touch_ask) = match ctx.book(ticker) {
            Some(b) => (b.best_bid().map(|x| x.0), b.best_ask().map(|x| x.0)),
            None => (None, None),
        };
        let fee = ctx.fee_model(ticker).fee_per_contract(Fp::from_f64(fair), true);

        // desired bid
        let mut want_bid: Option<Fp> = None;
        if pos.yes_qty.to_f64() < max_q {
            let raw = Fp::from_f64(fair - self.cfg.min_edge - fee);
            let mut px = raw.round_down_to(kalshi_tick(raw));
            if let Some(ta) = touch_ask
                && px >= ta
            {
                px = ta - kalshi_tick(ta);
            }
            if px > Fp::TICK && px < Fp::ONE - Fp::TICK {
                want_bid = Some(px);
            }
        }
        // desired ask
        let mut want_ask: Option<Fp> = None;
        if pos.yes_qty.to_f64() > -max_q {
            let raw = Fp::from_f64(fair + self.cfg.min_edge + fee);
            let mut px = raw.round_up_to(kalshi_tick(raw));
            if let Some(tb) = touch_bid
                && px <= tb
            {
                px = tb + kalshi_tick(tb);
            }
            if px > Fp::TICK && px < Fp::ONE - Fp::TICK {
                want_ask = Some(px);
            }
        }

        let qty = Fp::from_f64(self.cfg.maker_qty);
        let Some(a) = self.active.get_mut(ticker) else { return };

        match (a.bid_order, want_bid) {
            (Some((_, px)), Some(w)) if px == w => {}
            (cur, w) => {
                if let Some((id, _)) = cur {
                    ctx.cancel(id);
                    a.bid_order = None;
                }
                if let Some(w) = w {
                    let id = ctx.submit(OrderRequest::buy_yes(ticker, w, qty, Tif::Gtc).post_only().tagged("mk_bid"));
                    a.bid_order = Some((id, w));
                    self.stats.orders += 1;
                }
            }
        }
        match (a.ask_order, want_ask) {
            (Some((_, px)), Some(w)) if px == w => {}
            (cur, w) => {
                if let Some((id, _)) = cur {
                    ctx.cancel(id);
                    a.ask_order = None;
                }
                if let Some(w) = w {
                    let id = ctx.submit(OrderRequest::sell_yes(ticker, w, qty, Tif::Gtc).post_only().tagged("mk_ask"));
                    a.ask_order = Some((id, w));
                    self.stats.orders += 1;
                }
            }
        }
    }
}

impl Strategy for Btc15mStrategy {
    fn name(&self) -> &str {
        if self.cfg.maker { "btc15m_fair_value_maker" } else { "btc15m_fair_value_taker" }
    }

    fn on_event(&mut self, ev: &MarketEvent, ctx: &mut dyn Context) {
        match ev {
            MarketEvent::Market(m) => self.on_market(m),
            MarketEvent::Ref(r) => {
                if r.symbol != self.cfg.ref_symbol {
                    return;
                }
                self.vol.on_ref(r.ts_ms, r.px);
                self.basis.on_ref(r.ts_ms, r.px);
                self.spot = Some((r.ts_ms, r.px));
                if let Some(a) = r.avg_60s {
                    self.official_avg = Some((r.ts_ms, a));
                }
                self.recent.push_back((r.ts_ms, r.px));
                while self.recent.front().map(|(t, _)| r.ts_ms - *t > 120_000).unwrap_or(false) {
                    self.recent.pop_front();
                }
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
                if let Some(a) = self.active.get(&t.ticker) {
                    // every fresh print teaches the implied-vol series
                    if let Some((sts, s)) = self.spot
                        && t.ts_ms - sts <= 10_000
                    {
                        let tau = (a.close_ts_ms - t.ts_ms) as f64 / 1000.0;
                        if tau >= 120.0 {
                            let b = self.effective_basis();
                            self.vol.on_print(s + b, a.strike, t.yes_px.to_f64(), tau, self.cfg.settle_avg_secs);
                        }
                    }
                    let tk = t.ticker.clone();
                    self.evaluate(&tk, ctx);
                }
            }
            MarketEvent::Settlement { ticker, .. } => {
                self.active.remove(ticker);
            }
            MarketEvent::UserFill(_) => {}
        }
    }

    fn snapshot(&self) -> serde_json::Value {
        let now = self.spot.map(|(t, _)| t).unwrap_or(0);
        let markets: Vec<serde_json::Value> = self
            .active
            .iter()
            .map(|(t, a)| {
                serde_json::json!({
                    "ticker": t,
                    "strike": a.strike,
                    "open_ts_ms": a.open_ts_ms,
                    "close_ts_ms": a.close_ts_ms,
                    "fair": self.fair(t, now),
                    "entries": a.entries,
                    "notional": a.notional,
                    "bid_quote": a.bid_order.map(|(_, p)| p.to_f64()),
                    "ask_quote": a.ask_order.map(|(_, p)| p.to_f64()),
                })
            })
            .collect();
        serde_json::json!({
            "kind": "btc15m",
            "mode": if self.cfg.maker { "maker" } else { "taker" },
            "series": self.cfg.series,
            "spot": self.spot.map(|(_, p)| p),
            "spot_ts_ms": self.spot.map(|(t, _)| t),
            "sigma_annual": self.vol.sigma_annual(),
            "vol_samples": self.vol.realized.samples(),
            "vol_min_samples": self.cfg.vol_min_samples,
            "implied_annual": self.vol.implied_annual(),
            "basis": self.effective_basis(),
            "basis_samples": self.basis.samples(),
            "endgame": self.cfg.endgame,
            "official_avg_60s": self.official_avg.map(|(_, a)| a),
            "ref_symbol": self.cfg.ref_symbol,
            "market_blend": self.cfg.market_blend,
            "min_edge": self.cfg.min_edge,
            "stats": {"evaluations": self.stats.evaluations, "signals": self.stats.signals, "orders": self.stats.orders},
            "markets": markets,
        })
    }

    fn on_fill(&mut self, fill: &Fill, _ctx: &mut dyn Context) {
        if let Some(a) = self.active.get_mut(&fill.ticker) {
            let cost = match fill.action {
                Action::Buy => fill.yes_px.mul(fill.qty),
                Action::Sell => fill.yes_px.complement().mul(fill.qty),
            };
            a.notional += cost.to_f64() + fill.fee.to_f64();
            // a fully-filled resting quote is gone; re-evaluation will re-quote
            if fill.is_maker {
                match fill.action {
                    Action::Buy => {
                        if a.bid_order.is_some_and(|(id, _)| id == fill.order_id) && _ctx.open_orders(&fill.ticker).iter().all(|(id, _)| *id != fill.order_id) {
                            a.bid_order = None;
                        }
                    }
                    Action::Sell => {
                        if a.ask_order.is_some_and(|(id, _)| id == fill.order_id) && _ctx.open_orders(&fill.ticker).iter().all(|(id, _)| *id != fill.order_id) {
                            a.ask_order = None;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tapered_ticks() {
        assert_eq!(kalshi_tick(Fp::parse("0.05").unwrap()), Fp::TICK);
        assert_eq!(kalshi_tick(Fp::parse("0.50").unwrap()), Fp::CENT);
        assert_eq!(kalshi_tick(Fp::parse("0.95").unwrap()), Fp::TICK);
    }
}
