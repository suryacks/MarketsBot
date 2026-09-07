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
            }
            MarketEvent::BookDelta { ticker, side, px, delta, .. } => {
                self.books.entry(ticker.clone()).or_default().apply_delta(*side, *px, *delta);
            }
            MarketEvent::BookLevel { ticker, side, px, qty, .. } => {
                self.books.entry(ticker.clone()).or_default().set_level(*side, *px, *qty);
            }
            MarketEvent::Trade(t) => {
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
                self.resting.insert(id, Resting { req: req.clone(), remaining: req.qty });
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
            self.resting.insert(id, Resting { req, remaining });
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
            let hit = through || (at && hash01(&format!("{}{}", t.trade_id, id.0)) < self.cfg.maker_touch_fill_prob);
            if !hit {
                continue;
            }
            let q = self.affordable(r.req.action, r.req.yes_px, r.remaining.min(t.qty));
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
