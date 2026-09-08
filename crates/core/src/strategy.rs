use crate::book::Orderbook;
use crate::fees::FeeModel;
use crate::fp::Fp;
use crate::types::{MarketEvent, Outcome};

/// Buy or sell *YES* contracts. Buying NO at p is expressed as selling YES at 1-p.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Buy,
    Sell,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Tif {
    /// Immediate-or-cancel: fill what's available at or better than the limit, cancel the rest.
    Ioc,
    /// Good-till-canceled: rest on the book.
    Gtc,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OrderId(pub u64);

#[derive(Clone, Debug, PartialEq)]
pub struct OrderRequest {
    pub ticker: String,
    pub action: Action,
    /// Limit price in YES terms.
    pub yes_px: Fp,
    pub qty: Fp,
    pub tif: Tif,
    pub post_only: bool,
    /// Free-form label for analysis ("fv_buy", "arb_leg1", ...).
    pub tag: &'static str,
}

impl OrderRequest {
    pub fn buy_yes(ticker: impl Into<String>, yes_px: Fp, qty: Fp, tif: Tif) -> Self {
        Self {
            ticker: ticker.into(),
            action: Action::Buy,
            yes_px,
            qty,
            tif,
            post_only: false,
            tag: "",
        }
    }
    pub fn sell_yes(ticker: impl Into<String>, yes_px: Fp, qty: Fp, tif: Tif) -> Self {
        Self {
            ticker: ticker.into(),
            action: Action::Sell,
            yes_px,
            qty,
            tif,
            post_only: false,
            tag: "",
        }
    }
    /// Buy NO at `no_px` == sell YES at 1 - no_px.
    pub fn buy_no(ticker: impl Into<String>, no_px: Fp, qty: Fp, tif: Tif) -> Self {
        Self::sell_yes(ticker, no_px.complement(), qty, tif)
    }
    pub fn tagged(mut self, tag: &'static str) -> Self {
        self.tag = tag;
        self
    }
    pub fn post_only(mut self) -> Self {
        self.post_only = true;
        self
    }
    /// The venue-native (outcome, price) pair. Kalshi and Polymarket both accept
    /// YES-side orders directly, so this is mainly for logging.
    pub fn as_outcome(&self) -> (Outcome, Fp) {
        match self.action {
            Action::Buy => (Outcome::Yes, self.yes_px),
            Action::Sell => (Outcome::No, self.yes_px.complement()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Fill {
    pub order_id: OrderId,
    pub ticker: String,
    pub ts_ms: i64,
    pub action: Action,
    pub yes_px: Fp,
    pub qty: Fp,
    pub fee: Fp,
    pub is_maker: bool,
    pub tag: &'static str,
}

impl Fill {
    /// Signed change in YES inventory.
    pub fn yes_qty_delta(&self) -> Fp {
        match self.action {
            Action::Buy => self.qty,
            Action::Sell => -self.qty,
        }
    }
    /// Signed change in cash (negative = we paid), fees included.
    pub fn cash_delta(&self) -> Fp {
        let notional = self.yes_px.mul(self.qty);
        match self.action {
            Action::Buy => -notional - self.fee,
            Action::Sell => notional - self.fee,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Position {
    pub ticker: String,
    /// Net YES contracts (negative = net short YES = long NO).
    pub yes_qty: Fp,
    /// Cumulative cash flow from fills in this market (fees included).
    pub cash: Fp,
    pub fees: Fp,
    pub n_fills: u32,
    /// Gross contracts traded (|qty| summed).
    pub volume: Fp,
}

impl Position {
    pub fn apply(&mut self, f: &Fill) {
        self.yes_qty += f.yes_qty_delta();
        self.cash += f.cash_delta();
        self.fees += f.fee;
        self.n_fills += 1;
        self.volume += f.qty;
    }
    /// Cash received at settlement.
    pub fn settle(&self, result: Outcome) -> Fp {
        match result {
            Outcome::Yes => self.yes_qty,
            Outcome::No => Fp::ZERO,
        }
    }
    /// Mark-to-market PnL at a given YES price.
    pub fn mtm(&self, yes_px: Fp) -> Fp {
        self.cash + self.yes_qty.mul(yes_px)
    }
}

/// What a strategy can see and do. Implemented by the backtest simulator, the
/// paper trader and the live executor.
pub trait Context {
    fn now_ms(&self) -> i64;
    fn book(&self, ticker: &str) -> Option<&Orderbook>;
    fn position(&self, ticker: &str) -> Position;
    /// Free cash (bankroll) available for new positions.
    fn cash(&self) -> Fp;
    fn fee_model(&self, ticker: &str) -> FeeModel;
    fn submit(&mut self, req: OrderRequest) -> OrderId;
    fn cancel(&mut self, id: OrderId);
    fn open_orders(&self, ticker: &str) -> Vec<(OrderId, OrderRequest)>;
}

pub trait Strategy: Send {
    fn name(&self) -> &str;
    fn on_event(&mut self, ev: &MarketEvent, ctx: &mut dyn Context);
    fn on_fill(&mut self, _fill: &Fill, _ctx: &mut dyn Context) {}
    /// Free-form introspection for dashboards (per-market fair values, model state, …).
    fn snapshot(&self) -> serde_json::Value {
        serde_json::Value::Null
    }
}

/// Mark an open position: `(mark, unrealized_pnl, market_value)`.
///
/// The mark is the mid when both sides are quoted, otherwise whichever side is
/// quoted, otherwise the position's own average entry price — marking a position
/// we cannot see at zero would report a total loss on a market that has merely
/// stopped quoting. `market_value` is what the position is worth (negative for a
/// short); equity is cash + market_value. `unrealized_pnl` is value minus what we
/// paid, and must never be added to cash, which has already paid for the position.
pub fn mark_position(p: &Position, mid: Option<Fp>, bid: Option<Fp>, ask: Option<Fp>) -> (Option<f64>, f64, f64) {
    let q = p.yes_qty.to_f64();
    let entry = if q.abs() > 1e-9 { Some((-p.cash.to_f64() / q).clamp(0.0, 1.0)) } else { None };
    let mark = mid.map(|m| m.to_f64()).or_else(|| bid.map(|b| b.to_f64())).or_else(|| ask.map(|a| a.to_f64())).or(entry);
    match mark {
        Some(m) => (Some(m), p.cash.to_f64() + q * m, q * m),
        None => (None, 0.0, 0.0),
    }
}
