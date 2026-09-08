//! Simulated exchange implementing `Context`.
//!
//! Fill model:
//! * **Book mode** – we have full orderbooks (live paper trading or recorded
//!   books). IOC orders sweep the book up to the limit; resting orders fill
//!   when the tape prints through them (maker fee).
//! * **Tape mode** – we only have the historical trade tape (Kalshi REST
//!   history). A *synthetic touch* is inferred: the last price a YES-taker paid
//!   is the ask, the last price a NO-taker paid is the bid, each carrying the
//!   printed size and expiring after `touch_ttl_ms`. IOC orders can only
//!   consume the printed size, so we never fill more than the market actually
//!   showed. This is conservative on size and optimistic on price-stability
//!   within the TTL — recorded books remove the approximation.
//!
//! Latency: an order becomes active `latency_ms` after submission and is
//! matched against whatever the book looks like *then*.

use mb_core::{Action, BookSide, Context, FeeModel, Fill, Fp, MarketEvent, OrderId, OrderRequest, Orderbook, Outcome, Position, Tif};
use std::collections::{BTreeMap, HashMap};
use tracing::trace;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FillMode {
    Book,
    Tape,
}

#[derive(Clone, Debug)]
pub struct SimConfig {
    pub mode: FillMode,
    pub latency_ms: i64,
    /// Tape mode: how long an inferred touch stays valid.
    pub touch_ttl_ms: i64,
    pub initial_cash: Fp,
    pub default_fee: FeeModel,
    /// Probability that a resting order fills when the tape prints *at* its price
    /// (we don't know our queue position); prints *through* it always fill.
    pub maker_touch_fill_prob: f64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            mode: FillMode::Tape,
            latency_ms: 250,
            touch_ttl_ms: 2_000,
            initial_cash: Fp::from_int(1_000),
            default_fee: FeeModel::kalshi_default(),
            maker_touch_fill_prob: 0.5,
        }
    }
}

/// Deterministic pseudo-random in [0,1) from a trade id, so runs are reproducible.
fn hash01(s: &str) -> f64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    (h >> 11) as f64 / (1u64 << 53) as f64
}

#[derive(Clone, Debug)]
struct Touch {
    px: Fp,
    qty: Fp,
    ts_ms: i64,
}

#[derive(Clone, Debug, Default)]
struct SynthBook {
    bid: Option<Touch>,
    ask: Option<Touch>,
}

#[derive(Clone, Debug)]
struct Resting {
    req: OrderRequest,
    remaining: Fp,
    /// Book mode: contracts queued ahead of us at our price. Set from the book
    /// when the order becomes active; reduced by trades at our level (front of
    /// queue first) and pro-rata by cancels. We fill only once it reaches zero.
    ahead: Fp,
}

pub struct SimExchange {
    cfg: SimConfig,
    now_ms: i64,
    books: HashMap<String, Orderbook>,
    synth: HashMap<String, SynthBook>,
    positions: HashMap<String, Position>,
    cash: Fp,
    fee_models: HashMap<String, FeeModel>,
    pending: BTreeMap<(i64, u64), OrderRequest>,
    resting: BTreeMap<OrderId, Resting>,
    fills: Vec<Fill>,
    next_id: u64,
    pub settled: Vec<(String, Outcome, Position, Fp)>,
    /// (ts_ms, ticker, yes_px, qty) of recent trades, to tell trade-driven level
    /// reductions from cancels in book mode.
    recent_trades: std::collections::VecDeque<(i64, String, Fp, Fp)>,
    /// Negative level deltas already applied to queue positions, so the matching
    /// trade message (which may arrive after the delta) doesn't decrement twice.
    recent_deltas: std::collections::VecDeque<(i64, String, Fp, Fp)>,
    pub queue_stats: QueueStats,
}

#[derive(Clone, Debug, Default)]
pub struct QueueStats {
    pub orders_rested: u64,
    /// Sum of contracts ahead at insertion (for the average).
    pub ahead_at_insert: f64,
    pub reached_front: u64,
    pub fills_at_price: u64,
    pub fills_through: u64,
}

impl SimExchange {
    pub fn new(cfg: SimConfig) -> Self {
        Self {
            cash: cfg.initial_cash,
            cfg,
            now_ms: 0,
            books: HashMap::new(),
            synth: HashMap::new(),
            positions: HashMap::new(),
            fee_models: HashMap::new(),
            pending: BTreeMap::new(),
            resting: BTreeMap::new(),
            fills: Vec::new(),
            next_id: 1,
            settled: Vec::new(),
            recent_trades: std::collections::VecDeque::new(),
            recent_deltas: std::collections::VecDeque::new(),
            queue_stats: QueueStats::default(),
        }
    }

    fn remember(q: &mut std::collections::VecDeque<(i64, String, Fp, Fp)>, item: (i64, String, Fp, Fp)) {
        q.push_back(item);
        while q.len() > 512 {
            q.pop_front();
        }
    }

    fn recently(q: &std::collections::VecDeque<(i64, String, Fp, Fp)>, now: i64, ticker: &str, px: Fp, qty: Fp) -> bool {
        q.iter().any(|(ts, t, p, q)| now - *ts <= 2_000 && t == ticker && *p == px && *q == qty)
    }

    fn level_qty(&self, ticker: &str, action: Action, px: Fp) -> Fp {
        let Some(b) = self.books.get(ticker) else { return Fp::ZERO };
        match action {
            Action::Buy => b.bids.get(&px).copied().unwrap_or(Fp::ZERO),
            Action::Sell => b.asks.get(&px).copied().unwrap_or(Fp::ZERO),
        }
    }

    /// Book mode: a level we rest on shrank by `removed` (before the book was
    /// updated, the level held `before`). If a trade of that size just printed
    /// there, the Trade path has already moved the queue (trades eat the front);
    /// otherwise treat it as cancels spread uniformly through the queue.
    fn on_level_reduced(&mut self, ticker: &str, side: BookSide, px: Fp, removed: Fp, before: Fp) {
        if removed.0 <= 0 || before.0 <= 0 {
            return;
        }
        let now = self.now_ms;
        if Self::recently(&self.recent_trades, now, ticker, px, removed) {
            return;
        }
        Self::remember(&mut self.recent_deltas, (now, ticker.to_string(), px, removed));
        let action = match side {
            BookSide::Bid => Action::Buy,
            BookSide::Ask => Action::Sell,
        };
        for r in self.resting.values_mut() {
            if r.req.ticker != ticker || r.req.action != action || r.req.yes_px != px || r.ahead.0 <= 0 {
                continue;
            }
            let dec = Fp((removed.0 as i128 * r.ahead.0 as i128 / before.0 as i128) as i64);
            r.ahead = (r.ahead - dec).max(Fp::ZERO);
            if r.ahead.is_zero() {
                self.queue_stats.reached_front += 1;
            }
        }
    }

    pub fn set_fee_model(&mut self, ticker: &str, fm: FeeModel) {
        self.fee_models.insert(ticker.to_string(), fm);
    }

    pub fn total_cash(&self) -> Fp {
        self.cash
    }

    /// Cash not locked as collateral. A short YES position of q contracts must be
    /// able to pay $1·q at settlement; since selling YES at p already credited
    /// p·q, the locked amount is exactly q (== Kalshi's (1−p)·q collateral on the
    /// original balance). Long positions are already paid for.
    pub fn free_cash(&self) -> Fp {
        let locked: Fp = self.positions.values().filter(|p| p.yes_qty.0 < 0).map(|p| -p.yes_qty).sum();
        self.cash - locked
    }

    /// Largest quantity we can afford at this price/side.
    fn affordable(&self, action: Action, px: Fp, qty: Fp) -> Fp {
        let free = self.free_cash();
        if free.0 <= 0 {
            return Fp::ZERO;
        }
        let unit = match action {
            Action::Buy => px,
            Action::Sell => px.complement(),
        };
        if unit.0 <= 0 {
            return qty;
        }
        // truncate to whole contracts so tiny remainders don't churn
        qty.min(free.div(unit).round_down_to(Fp::ONE))
    }

    pub fn positions(&self) -> &HashMap<String, Position> {
        &self.positions
    }

    pub fn books(&self) -> &HashMap<String, Orderbook> {
        &self.books
    }

    /// Every resting order: (id, request, remaining, contracts ahead in queue).
    pub fn resting_orders(&self) -> Vec<(OrderId, OrderRequest, Fp, Fp)> {
        self.resting.iter().map(|(id, r)| (*id, r.req.clone(), r.remaining, r.ahead)).collect()
    }

    pub fn drain_fills(&mut self) -> Vec<Fill> {
        std::mem::take(&mut self.fills)
    }

    /// Advance the clock and apply a market event to the simulated state.
    pub fn on_event(&mut self, ev: &MarketEvent) {
        let ts = ev.ts_ms();
        if ts > self.now_ms {
            self.now_ms = ts;
        }
        match ev {
            MarketEvent::BookSnapshot {
                ticker, bids, asks, ts_ms, seq, ..
            } => {
                self.books.entry(ticker.clone()).or_default().replace(bids, asks, *ts_ms, *seq);
                // resync: nobody can be ahead of us beyond what the level now holds
                let levels: Vec<(OrderId, Fp)> = self
                    .resting
                    .iter()
                    .filter(|(_, r)| &r.req.ticker == ticker)
                    .map(|(id, r)| (*id, self.level_qty(ticker, r.req.action, r.req.yes_px)))
                    .collect();
                for (id, lvl) in levels {
                    if let Some(r) = self.resting.get_mut(&id) {
                        r.ahead = r.ahead.min(lvl);
                    }
                }
            }
            MarketEvent::BookDelta { ticker, side, px, delta, .. } => {
                if delta.0 < 0 && self.cfg.mode == FillMode::Book {
                    let before = self.level_qty(ticker, if *side == BookSide::Bid { Action::Buy } else { Action::Sell }, *px);
                    self.on_level_reduced(ticker, *side, *px, -*delta, before);
                }
                self.books.entry(ticker.clone()).or_default().apply_delta(*side, *px, *delta);
            }
            MarketEvent::BookLevel { ticker, side, px, qty, .. } => {
                if self.cfg.mode == FillMode::Book {
                    let before = self.level_qty(ticker, if *side == BookSide::Bid { Action::Buy } else { Action::Sell }, *px);
                    if *qty < before {
                        self.on_level_reduced(ticker, *side, *px, before - *qty, before);
                    }
                }
                self.books.entry(ticker.clone()).or_default().set_level(*side, *px, *qty);
            }
            // Touch-only feed (ticker channel): keep a one-level book so taker strategies can see
            // the touch. Assumed depth 100 contracts — fine for $2 stakes, not for size.
            MarketEvent::Ticker { ticker, ts_ms, yes_bid, yes_ask, .. } => {
                let synthetic = self.books.get(ticker).map(|b| b.bids.len() <= 1 && b.asks.len() <= 1).unwrap_or(true);
                if synthetic && self.cfg.mode == FillMode::Book {
                    let b = self.books.entry(ticker.clone()).or_default();
                    b.clear();
                    if let Some(p) = yes_bid.filter(|p| p.is_positive()) {
                        b.bids.insert(p, Fp::from_int(100));
                    }
                    if let Some(p) = yes_ask.filter(|p| p.is_positive() && *p < Fp::ONE) {
                        b.asks.insert(p, Fp::from_int(100));
                    }
                    b.ts_ms = *ts_ms;
                }
            }
            MarketEvent::Trade(t) => {
                if self.cfg.mode == FillMode::Book {
                    Self::remember(&mut self.recent_trades, (self.now_ms, t.ticker.clone(), t.yes_px, t.qty));
                }
                if self.cfg.mode == FillMode::Tape {
                    let sb = self.synth.entry(t.ticker.clone()).or_default();
                    let touch = Touch {
                        px: t.yes_px,
                        qty: t.qty,
                        ts_ms: t.ts_ms,
                    };
                    match t.taker {
                        Outcome::Yes => sb.ask = Some(touch),
                        Outcome::No => sb.bid = Some(touch),
                    }
                    self.refresh_synth_book(&t.ticker);
                }
                self.match_resting_against_trade(t);
            }
            _ => {}
        }
        self.activate_pending();
    }

    /// Materialize the synthetic touch as a one-level book so strategies can
    /// use the same `ctx.book()` API in both modes.
    fn refresh_synth_book(&mut self, ticker: &str) {
        let Some(sb) = self.synth.get(ticker) else { return };
        let now = self.now_ms;
        let ttl = self.cfg.touch_ttl_ms;
        let mut ob = Orderbook::new();
        if let Some(b) = &sb.bid
            && now - b.ts_ms <= ttl
            && b.qty.is_positive()
        {
            ob.bids.insert(b.px, b.qty);
        }
        if let Some(a) = &sb.ask
            && now - a.ts_ms <= ttl
            && a.qty.is_positive()
        {
            // never show a crossed synthetic book
            if ob.best_bid().is_none_or(|(bp, _)| a.px > bp) {
                ob.asks.insert(a.px, a.qty);
            } else {
                ob.bids.clear();
                ob.asks.insert(a.px, a.qty);
            }
        }
        ob.ts_ms = now;
        self.books.insert(ticker.to_string(), ob);
    }

    fn activate_pending(&mut self) {
        loop {
            let Some((&(at, id), _)) = self.pending.iter().next() else { break };
            if at > self.now_ms {
                break;
            }
            let req = self.pending.remove(&(at, id)).unwrap();
            self.execute(OrderId(id), req);
        }
    }

    fn fee_for(&self, ticker: &str) -> FeeModel {
        self.fee_models.get(ticker).cloned().unwrap_or_else(|| self.cfg.default_fee.clone())
    }

    fn record_fill(&mut self, id: OrderId, req: &OrderRequest, px: Fp, qty: Fp, is_maker: bool) {
        let fee = self.fee_for(&req.ticker).fee(px, qty, is_maker);
        let fill = Fill {
            order_id: id,
            ticker: req.ticker.clone(),
            ts_ms: self.now_ms,
            action: req.action,
            yes_px: px,
            qty,
            fee,
            is_maker,
            tag: req.tag,
        };
        self.cash += fill.cash_delta();
        let pos = self.positions.entry(req.ticker.clone()).or_insert_with(|| Position {
            ticker: req.ticker.clone(),
            ..Default::default()
        });
        pos.apply(&fill);
        trace!(?fill, "fill");
        self.fills.push(fill);
    }

    fn execute(&mut self, id: OrderId, req: OrderRequest) {
        if self.cfg.mode == FillMode::Tape {
            self.refresh_synth_book(&req.ticker);
        }
        let Some(book) = self.books.get(&req.ticker) else {
            if req.tif == Tif::Gtc {
                self.queue_stats.orders_rested += 1;
                self.resting.insert(id, Resting { req: req.clone(), remaining: req.qty, ahead: Fp::ZERO });
            }
            return;
        };
        let side = match req.action {
            Action::Buy => BookSide::Ask,
            Action::Sell => BookSide::Bid,
        };
        let sweep = if req.post_only { Default::default() } else { book.sweep(side, req.qty, Some(req.yes_px)) };
        let mut remaining = req.qty;
        for (px, q) in sweep.levels.iter() {
            let q = &self.affordable(req.action, *px, *q);
            if q.0 <= 0 {
                break;
            }
            self.record_fill(id, &req, *px, *q, false);
            remaining -= *q;
            // consume liquidity
            if let Some(b) = self.books.get_mut(&req.ticker) {
                b.apply_delta(side, *px, -*q);
            }
            if self.cfg.mode == FillMode::Tape
                && let Some(sb) = self.synth.get_mut(&req.ticker)
            {
                let t = match side {
                    BookSide::Ask => &mut sb.ask,
                    BookSide::Bid => &mut sb.bid,
                };
                if let Some(t) = t {
                    t.qty -= *q;
                }
            }
        }
        if remaining.is_positive() && req.tif == Tif::Gtc {
            // everyone already at our price is ahead of us
            let ahead = if self.cfg.mode == FillMode::Book { self.level_qty(&req.ticker, req.action, req.yes_px) } else { Fp::ZERO };
            self.queue_stats.orders_rested += 1;
            self.queue_stats.ahead_at_insert += ahead.to_f64();
            if ahead.is_zero() {
                self.queue_stats.reached_front += 1;
            }
            self.resting.insert(id, Resting { req, remaining, ahead });
        }
    }

    /// A resting buy at P fills when a NO-taker trade prints at ≤ P (someone
    /// sold YES into the book at our level or better); a resting sell at P fills
    /// when a YES-taker prints at ≥ P.
    fn match_resting_against_trade(&mut self, t: &mb_core::Trade) {
        let ids: Vec<OrderId> = self.resting.iter().filter(|(_, r)| r.req.ticker == t.ticker).map(|(id, _)| *id).collect();
        for id in ids {
            let r = self.resting.get(&id).unwrap().clone();
            let (through, at) = match (r.req.action, t.taker) {
                (Action::Buy, Outcome::No) => (t.yes_px < r.req.yes_px, t.yes_px == r.req.yes_px),
                (Action::Sell, Outcome::Yes) => (t.yes_px > r.req.yes_px, t.yes_px == r.req.yes_px),
                _ => (false, false),
            };
            // How much of this print reaches us.
            let reach = if through {
                t.qty
            } else if !at {
                Fp::ZERO
            } else if self.cfg.mode == FillMode::Book {
                // the trade eats the queue ahead of us first — unless its level delta
                // already arrived and was applied (pro-rata) as if it were a cancel
                let ahead = r.ahead;
                let already = Self::recently(&self.recent_deltas, self.now_ms, &t.ticker, t.yes_px, t.qty);
                let reach = (t.qty - ahead).max(Fp::ZERO);
                if !already && let Some(rr) = self.resting.get_mut(&id) {
                    rr.ahead = (ahead - t.qty).max(Fp::ZERO);
                    if ahead.is_positive() && rr.ahead.is_zero() {
                        self.queue_stats.reached_front += 1;
                    }
                }
                reach
            } else if hash01(&format!("{}{}", t.trade_id, id.0)) < self.cfg.maker_touch_fill_prob {
                t.qty
            } else {
                Fp::ZERO
            };
            if reach.0 <= 0 {
                continue;
            }
            if through {
                self.queue_stats.fills_through += 1;
            } else {
                self.queue_stats.fills_at_price += 1;
            }
            let q = self.affordable(r.req.action, r.req.yes_px, r.remaining.min(reach));
            if q.0 <= 0 {
                continue;
            }
            self.record_fill(id, &r.req, r.req.yes_px, q, true);
            let rem = r.remaining - q;
            if rem.is_positive() {
                self.resting.get_mut(&id).unwrap().remaining = rem;
            } else {
                self.resting.remove(&id);
            }
        }
    }

    /// Settle a market: pay out YES contracts, cancel resting orders, record PnL.
    pub fn settle(&mut self, ticker: &str, result: Outcome) -> Option<Fp> {
        self.resting.retain(|_, r| r.req.ticker != ticker);
        self.pending.retain(|_, r| r.ticker != ticker);
        self.books.remove(ticker);
        self.synth.remove(ticker);
        let pos = self.positions.remove(ticker)?;
        let payout = pos.settle(result);
        self.cash += payout;
        let pnl = pos.cash + payout;
        self.settled.push((ticker.to_string(), result, pos, pnl));
        Some(pnl)
    }
}

impl Context for SimExchange {
    fn now_ms(&self) -> i64 {
        self.now_ms
    }
    fn book(&self, ticker: &str) -> Option<&Orderbook> {
        self.books.get(ticker)
    }
    fn position(&self, ticker: &str) -> Position {
        self.positions.get(ticker).cloned().unwrap_or_else(|| Position {
            ticker: ticker.to_string(),
            ..Default::default()
        })
    }
    fn cash(&self) -> Fp {
        self.free_cash()
    }
    fn fee_model(&self, ticker: &str) -> FeeModel {
        self.fee_for(ticker)
    }
    fn submit(&mut self, req: OrderRequest) -> OrderId {
        let id = self.next_id;
        self.next_id += 1;
        let at = self.now_ms + self.cfg.latency_ms;
        self.pending.insert((at, id), req);
        if self.cfg.latency_ms == 0 {
            self.activate_pending();
        }
        OrderId(id)
    }
    fn cancel(&mut self, id: OrderId) {
        self.resting.remove(&id);
        self.pending.retain(|(_, i), _| *i != id.0);
    }
    fn open_orders(&self, ticker: &str) -> Vec<(OrderId, OrderRequest)> {
        self.resting
            .iter()
            .filter(|(_, r)| r.req.ticker == ticker)
            .map(|(id, r)| (*id, r.req.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mb_core::{Trade, Venue};

    fn fp(s: &str) -> Fp {
        Fp::parse(s).unwrap()
    }

    fn trade(ts: i64, px: &str, qty: &str, taker: Outcome) -> MarketEvent {
        MarketEvent::Trade(Trade {
            venue: Venue::Kalshi,
            ticker: "T".into(),
            ts_ms: ts,
            yes_px: fp(px),
            qty: fp(qty),
            taker,
            trade_id: String::new(),
        })
    }

    #[test]
    fn tape_mode_ioc_fills_only_printed_size_after_latency() {
        let mut sim = SimExchange::new(SimConfig {
            latency_ms: 100,
            default_fee: FeeModel::None,
            ..Default::default()
        });
        sim.on_event(&trade(1000, "0.40", "10", Outcome::Yes)); // ask = 0.40 x 10
        let id = sim.submit(OrderRequest::buy_yes("T", fp("0.40"), fp("25"), Tif::Ioc));
        assert!(sim.drain_fills().is_empty()); // not active yet
        sim.on_event(&trade(1100, "0.41", "1", Outcome::No)); // bid = 0.41? crossed -> ask wins
        let fills = sim.drain_fills();
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].order_id, id);
        assert_eq!(fills[0].qty, fp("10"));
        assert_eq!(sim.position("T").yes_qty, fp("10"));
        assert_eq!(sim.cash(), Fp::from_int(1000) - fp("4"));
        // settle YES -> +10
        let pnl = sim.settle("T", Outcome::Yes).unwrap();
        assert_eq!(pnl, fp("6"));
    }

    #[test]
    fn cannot_exceed_bankroll() {
        let mut sim = SimExchange::new(SimConfig {
            latency_ms: 0,
            default_fee: FeeModel::None,
            initial_cash: Fp::from_int(10),
            ..Default::default()
        });
        sim.on_event(&trade(1000, "0.50", "100", Outcome::Yes)); // ask 0.50 x 100
        sim.submit(OrderRequest::buy_yes("T", fp("0.50"), fp("100"), Tif::Ioc));
        sim.on_event(&trade(1001, "0.50", "1", Outcome::Yes));
        let f = sim.drain_fills();
        assert_eq!(f[0].qty, fp("20")); // $10 / 0.50
        assert_eq!(sim.free_cash(), Fp::ZERO);
        // selling YES locks $1 per contract: with $0 free nothing fills
        sim.submit(OrderRequest::sell_yes("T", fp("0.50"), fp("5"), Tif::Ioc));
        sim.on_event(&trade(1002, "0.50", "5", Outcome::No));
        assert!(sim.drain_fills().is_empty());
    }

    #[test]
    fn book_mode_queue_position() {
        let mut sim = SimExchange::new(SimConfig {
            mode: FillMode::Book,
            latency_ms: 0,
            default_fee: FeeModel::None,
            ..Default::default()
        });
        // bid level 0.30 holds 100 contracts before we join with 10
        sim.on_event(&MarketEvent::BookSnapshot {
            venue: Venue::Kalshi,
            ticker: "T".into(),
            ts_ms: 1,
            seq: 1,
            bids: vec![(fp("0.30"), fp("100"))],
            asks: vec![(fp("0.32"), fp("50"))],
        });
        sim.submit(OrderRequest::buy_yes("T", fp("0.30"), fp("10"), Tif::Gtc).post_only());
        assert_eq!(sim.queue_stats.orders_rested, 1);
        // a 60-lot seller hits the level: all of it goes to the 100 ahead of us
        sim.on_event(&trade(10, "0.30", "60", Outcome::No));
        sim.on_event(&MarketEvent::BookDelta { venue: Venue::Kalshi, ticker: "T".into(), ts_ms: 10, seq: 2, side: BookSide::Bid, px: fp("0.30"), delta: fp("-60") });
        assert!(sim.drain_fills().is_empty());
        // 30 cancelled pro-rata: ahead 40 -> 40 - 30*40/40 ... level before = 40 (100-60), ahead=40 => ahead 10
        sim.on_event(&MarketEvent::BookDelta { venue: Venue::Kalshi, ticker: "T".into(), ts_ms: 11, seq: 3, side: BookSide::Bid, px: fp("0.30"), delta: fp("-30") });
        assert_eq!(sim.resting.values().next().unwrap().ahead, fp("10"));
        // a 25-lot seller: 10 to the queue ahead, 15 reach us, capped at our 10
        sim.on_event(&trade(20, "0.30", "25", Outcome::No));
        let f = sim.drain_fills();
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].qty, fp("10"));
        assert!(f[0].is_maker);
        assert_eq!(sim.queue_stats.fills_at_price, 1);
    }

    #[test]
    fn resting_order_fills_on_tape_as_maker() {
        let mut sim = SimExchange::new(SimConfig {
            latency_ms: 0,
            default_fee: FeeModel::kalshi("quadratic_with_maker_fees", 1.0),
            maker_touch_fill_prob: 1.0,
            ..Default::default()
        });
        sim.submit(OrderRequest::buy_yes("T", fp("0.30"), fp("5"), Tif::Gtc));
        sim.on_event(&trade(10, "0.31", "5", Outcome::No)); // seller hit 0.31 > our 0.30: no fill
        assert!(sim.drain_fills().is_empty());
        sim.on_event(&trade(20, "0.30", "3", Outcome::No));
        let f = sim.drain_fills();
        assert_eq!(f.len(), 1);
        assert!(f[0].is_maker);
        assert_eq!(f[0].qty, fp("3"));
        assert_eq!(sim.open_orders("T")[0].1.qty, fp("5"));
    }
}
