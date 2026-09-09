//! Live execution on Kalshi. `KalshiExecutor` implements `Context` so any
//! `Strategy` runs unchanged; orders go out through a worker task (REST), fills
//! come back on the WebSocket `fill` channel, and every order passes hard risk
//! limits first. Positions are seeded from the exchange and updated from fills.
//!
//! Limits (all enforced *before* an order leaves the process):
//! * `max_notional`: worst-case dollars at risk across positions + resting orders;
//! * `max_order_qty`, `max_open_orders`;
//! * `max_loss`: once equity − initial ≤ −max_loss the executor halts and
//!   cancels everything (kill switch).

use anyhow::Result;
use mb_core::{Action, Context, FeeModel, Fill, Fp, MarketEvent, OrderId, OrderRequest, Orderbook, Outcome, Position, Tif};
use mb_kalshi::rest::order_request;
use mb_kalshi::KalshiClient;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

#[derive(Clone, Debug)]
pub struct LiveConfig {
    pub max_notional: f64,
    pub max_order_qty: f64,
    pub max_open_orders: usize,
    pub max_loss: f64,
    /// Log orders instead of sending them.
    pub dry_run: bool,
}

impl Default for LiveConfig {
    fn default() -> Self {
        Self {
            max_notional: 100.0,
            max_order_qty: 5.0,
            max_open_orders: 20,
            max_loss: 50.0,
            dry_run: false,
        }
    }
}

enum Cmd {
    Submit { id: OrderId, req: OrderRequest },
    Cancel { kalshi_id: String },
}

enum Outcome_ {
    Placed { id: OrderId, kalshi_id: String },
    Failed { id: OrderId, err: String },
}

#[derive(Clone, Debug)]
struct LiveOrder {
    req: OrderRequest,
    kalshi_id: Option<String>,
    remaining: Fp,
    cancel_requested: bool,
}

pub struct KalshiExecutor {
    cfg: LiveConfig,
    client: KalshiClient,
    now_ms: i64,
    books: HashMap<String, Orderbook>,
    /// Kalshi's own valuation of open positions, in dollars (from the balance call).
    portfolio_value: f64,
    positions: HashMap<String, Position>,
    cash: Fp,
    initial_cash: Fp,
    fee_models: HashMap<String, FeeModel>,
    orders: HashMap<OrderId, LiveOrder>,
    next_id: u64,
    cmd_tx: mpsc::UnboundedSender<Cmd>,
    res_rx: mpsc::UnboundedReceiver<Outcome_>,
    fills: Vec<Fill>,
    seen_trades: HashSet<String>,
    pub halted: bool,
    pub rejected: u64,
    pub sent: u64,
}

impl KalshiExecutor {
    pub async fn new(client: KalshiClient, cfg: LiveConfig) -> Result<Self> {
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<Cmd>();
        let (res_tx, res_rx) = mpsc::unbounded_channel::<Outcome_>();
        let worker = client.clone();
        let dry = cfg.dry_run;
        tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    Cmd::Submit { id, req } => {
                        let kreq = order_request(&req.ticker, req.action, req.yes_px, req.qty, req.tif, req.post_only, Some(format!("mb-{}", id.0)));
                        if dry {
                            info!(?kreq, "DRY RUN order");
                            let _ = res_tx.send(Outcome_::Placed { id, kalshi_id: format!("dry-{}", id.0) });
                            continue;
                        }
                        match worker.create_order(&kreq).await {
                            Ok(r) => {
                                info!(id = id.0, kalshi_id = %r.order_id, ticker = %req.ticker, action = ?req.action, px = %req.yes_px, qty = %req.qty, filled = ?r.fill_count, "order placed");
                                let _ = res_tx.send(Outcome_::Placed { id, kalshi_id: r.order_id });
                            }
                            Err(e) => {
                                error!(id = id.0, error = %e, "order rejected");
                                let _ = res_tx.send(Outcome_::Failed { id, err: e.to_string() });
                            }
                        }
                    }
                    Cmd::Cancel { kalshi_id } => {
                        if dry {
                            continue;
                        }
                        if let Err(e) = worker.cancel_order(&kalshi_id).await {
                            warn!(%kalshi_id, error = %e, "cancel failed");
                        }
                    }
                }
            }
        });

        let mut ex = Self {
            cfg,
            client,
            now_ms: chrono::Utc::now().timestamp_millis(),
            books: HashMap::new(),
            portfolio_value: 0.0,
            positions: HashMap::new(),
            cash: Fp::ZERO,
            initial_cash: Fp::ZERO,
            fee_models: HashMap::new(),
            orders: HashMap::new(),
            next_id: 1,
            cmd_tx,
            res_rx,
            fills: Vec::new(),
            seen_trades: HashSet::new(),
            halted: false,
            rejected: 0,
            sent: 0,
        };
        ex.sync_account().await?;
        // Baseline is what the account is WORTH at start-up, not just its cash. After a
        // restart the positions are still there but the cash that bought them is gone, so
        // a cash-only baseline reports the value of pre-existing positions as fresh profit.
        ex.initial_cash = ex.cash + Fp::from_f64(ex.portfolio_value);
        Ok(ex)
    }

    pub fn set_fee_model(&mut self, series: &str, fm: FeeModel) {
        self.fee_models.insert(series.to_string(), fm);
    }

    /// Pull balance + positions from the exchange (start-up and periodic resync).
    pub async fn sync_account(&mut self) -> Result<()> {
        let bal = self.client.get_balance().await?;
        // Cash is the balance across ALL exchange shards, not `balance_breakdown[0]`.
        // Kalshi holds collateral per shard, so a bot trading crypto (shard 2) that read
        // entry 0 would be watching the weather shard: its own fills would never show up
        // in cash, and every resync would erase the P&L its kill switch depends on.
        let dollars = bal["balance_dollars"]
            .as_str()
            .and_then(|s| Fp::parse(s).ok())
            .or_else(|| {
                bal["balance_breakdown"].as_array().map(|bs| {
                    bs.iter().filter_map(|b| b["balance"].as_str()).filter_map(|s| Fp::parse(s).ok()).sum()
                })
            })
            .or_else(|| bal["balance"].as_i64().map(|c| Fp::from_f64(c as f64 / 100.0)))
            .unwrap_or(Fp::ZERO);
        self.cash = dollars;
        self.portfolio_value = bal["portfolio_value"].as_f64().map(|c| c / 100.0).unwrap_or(0.0);
        let pos = self.client.get_positions().await?;
        if let Some(mps) = pos["market_positions"].as_array() {
            for mp in mps {
                let t = mp["ticker"].as_str().unwrap_or("").to_string();
                let q = mp["position_fp"].as_str().and_then(|s| Fp::parse(s).ok()).unwrap_or(Fp::ZERO);
                if q.is_zero() {
                    continue;
                }
                let p = self.positions.entry(t.clone()).or_insert_with(|| Position {
                    ticker: t.clone(),
                    ..Default::default()
                });
                p.yes_qty = q;
                // Restore the cost basis, which does not survive a restart. Kalshi reports
                // exposure, so the average price paid is exposure/qty when long and
                // 1 - exposure/|qty| when short (the collateral side of the same trade).
                let exposure = mp["market_exposure_dollars"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                let qf = q.to_f64();
                if exposure > 0.0 && qf.abs() > 1e-9 {
                    let px_avg = if qf > 0.0 { exposure / qf } else { 1.0 - exposure / -qf };
                    p.cash = Fp::from_f64(-px_avg * qf);
                }
                if let Some(f) = mp["fees_paid_dollars"].as_str().and_then(|s| s.parse::<f64>().ok()) {
                    p.fees = Fp::from_f64(f);
                }
            }
        }
        info!(cash = %self.cash, positions = self.positions.len(), "account synced");
        Ok(())
    }

    fn fee_for(&self, ticker: &str) -> FeeModel {
        let series = ticker.split('-').next().unwrap_or("");
        self.fee_models.get(series).cloned().unwrap_or_else(FeeModel::kalshi_default)
    }

    /// Worst-case dollars at risk: open positions (|q| × $1) + resting orders (cost if filled).
    pub fn notional_at_risk(&self) -> f64 {
        let pos: f64 = self.positions.values().map(|p| p.yes_qty.abs().to_f64()).sum();
        let orders: f64 = self
            .orders
            .values()
            .map(|o| match o.req.action {
                Action::Buy => o.remaining.to_f64() * o.req.yes_px.to_f64(),
                Action::Sell => o.remaining.to_f64() * (1.0 - o.req.yes_px.to_f64()),
            })
            .sum();
        pos + orders
    }

    /// `(mark, unrealized_pnl, liquidation_value)` for one live position.
    ///
    /// `liquidation_value` follows the exchange, not the simulator. Kalshi debits the
    /// full $1 collateral when a short is opened, so the balance we mirror is already
    /// net of it and a short YES position is worth `qty × (1 − mark)` on top of that
    /// balance. Valuing it at `qty × mark` (the simulator's convention, where the
    /// premium was credited instead) would understate equity by the collateral and
    /// could trip the kill switch on a position that has not lost a cent.
    fn marked(&self, p: &mb_core::Position) -> (Option<f64>, f64, f64) {
        let bk = self.books.get(&p.ticker);
        let (mark, pnl, _) = mb_core::mark_position(p, bk.and_then(|b| b.mid()), bk.and_then(|b| b.best_bid()).map(|(x, _)| x), bk.and_then(|b| b.best_ask()).map(|(x, _)| x));
        let q = p.yes_qty.to_f64();
        let value = match mark {
            Some(m) if q < 0.0 => -q * (1.0 - m),
            Some(m) => q * m,
            None => 0.0,
        };
        (mark, pnl, value)
    }

    /// Cash movement in the exchange's own convention (see `marked`): opening a short
    /// costs collateral rather than crediting the premium, and closing one returns it.
    fn exchange_cash_delta(&self, fill: &Fill, prior_qty: Fp) -> f64 {
        let (q, px, prior) = (fill.qty.to_f64(), fill.yes_px.to_f64(), prior_qty.to_f64());
        let moved = match fill.action {
            Action::Buy => {
                let closing = q.min((-prior).max(0.0));
                closing * (1.0 - px) - (q - closing) * px
            }
            Action::Sell => {
                let closing = q.min(prior.max(0.0));
                closing * px - (q - closing) * (1.0 - px)
            }
        };
        moved - fill.fee.to_f64()
    }

    /// Unrealized P&L on open positions.
    pub fn unrealized(&self) -> f64 {
        self.positions.values().map(|p| self.marked(p).1).sum()
    }

    /// What the open positions are worth. Equity = cash + this; cash has already
    /// paid for them, so adding P&L instead would charge for them twice — and the
    /// kill switch would see a loss that never happened.
    pub fn positions_value(&self) -> f64 {
        self.positions.values().map(|p| self.marked(p).2).sum()
    }

    fn check_kill_switch(&mut self) {
        if self.halted {
            return;
        }
        let equity = self.cash.to_f64() + self.positions_value();
        if equity - self.initial_cash.to_f64() <= -self.cfg.max_loss {
            error!(equity, initial = %self.initial_cash, "KILL SWITCH: max loss reached — halting and cancelling all orders");
            self.halted = true;
            let ids: Vec<OrderId> = self.orders.keys().copied().collect();
            for id in ids {
                self.cancel(id);
            }
        }
    }

    /// Feed market/user events; call before the strategy sees the event.
    pub fn on_event(&mut self, ev: &MarketEvent) {
        self.now_ms = chrono::Utc::now().timestamp_millis();
        // worker results
        while let Ok(r) = self.res_rx.try_recv() {
            match r {
                Outcome_::Placed { id, kalshi_id } => {
                    if let Some(o) = self.orders.get_mut(&id) {
                        o.kalshi_id = Some(kalshi_id.clone());
                        if o.cancel_requested || o.req.tif == Tif::Ioc {
                            // IOC: whatever didn't fill is already gone; drop the shell after fills arrive
                            if o.cancel_requested {
                                let _ = self.cmd_tx.send(Cmd::Cancel { kalshi_id });
                            }
                        }
                    }
                }
                Outcome_::Failed { id, err } => {
                    warn!(id = id.0, %err, "order failed at the exchange");
                    self.orders.remove(&id);
                    self.rejected += 1;
                }
            }
        }
        match ev {
            MarketEvent::BookSnapshot { ticker, bids, asks, ts_ms, seq, .. } => {
                self.books.entry(ticker.clone()).or_default().replace(bids, asks, *ts_ms, *seq);
            }
            MarketEvent::BookDelta { ticker, side, px, delta, .. } => {
                self.books.entry(ticker.clone()).or_default().apply_delta(*side, *px, *delta);
            }
            MarketEvent::BookLevel { ticker, side, px, qty, .. } => {
                self.books.entry(ticker.clone()).or_default().set_level(*side, *px, *qty);
            }
            MarketEvent::UserFill(f) => {
                if !self.seen_trades.insert(f.trade_id.clone()) {
                    return;
                }
                // map back to our OrderId via the kalshi order id
                let our = self.orders.iter().find(|(_, o)| o.kalshi_id.as_deref() == Some(f.order_id.as_str())).map(|(id, o)| (*id, o.req.tag));
                let (id, tag) = our.unwrap_or((OrderId(0), "external"));
                let fill = Fill {
                    order_id: id,
                    ticker: f.ticker.clone(),
                    ts_ms: f.ts_ms,
                    action: if f.buy_yes { Action::Buy } else { Action::Sell },
                    yes_px: f.yes_px,
                    qty: f.qty,
                    fee: f.fee,
                    is_maker: !f.is_taker,
                    tag,
                };
                let prior_qty = self.positions.get(&f.ticker).map(|p| p.yes_qty).unwrap_or_default();
                self.cash += Fp::from_f64(self.exchange_cash_delta(&fill, prior_qty));
                let p = self.positions.entry(f.ticker.clone()).or_insert_with(|| Position {
                    ticker: f.ticker.clone(),
                    ..Default::default()
                });
                p.apply(&fill);
                if let Some(o) = self.orders.get_mut(&id) {
                    o.remaining -= f.qty;
                    if o.remaining.0 <= 0 {
                        self.orders.remove(&id);
                    }
                }
                info!(ticker = %f.ticker, action = ?fill.action, px = %f.yes_px, qty = %f.qty, fee = %f.fee, maker = fill.is_maker, "LIVE FILL");
                self.fills.push(fill);
                self.check_kill_switch();
            }
            MarketEvent::Settlement { ticker, result, .. } => {
                if let Some(p) = self.positions.remove(ticker) {
                    // Winning contracts return $1 each; the loser's collateral is already gone.
                    let q = p.yes_qty;
                    let payout = if *result == mb_core::Outcome::Yes { q.max(Fp::from_int(0)) } else { (-q).max(Fp::from_int(0)) };
                    self.cash += payout;
                    info!(ticker, ?result, pnl = %(p.cash + payout), "settled");
                }
                let ids: Vec<OrderId> = self.orders.iter().filter(|(_, o)| &o.req.ticker == ticker).map(|(id, _)| *id).collect();
                for id in ids {
                    self.orders.remove(&id);
                }
            }
            _ => {}
        }
    }

    pub fn drain_fills(&mut self) -> Vec<Fill> {
        std::mem::take(&mut self.fills)
    }

    pub fn open_orders_all(&self) -> Vec<OrderId> {
        self.orders.keys().copied().collect()
    }

    pub fn state(&self, run_id: &str, started_ms: i64, strategy_name: &str, strategy_state: serde_json::Value) -> serde_json::Value {
        use serde_json::json;
        let positions: Vec<_> = self
            .positions
            .values()
            .map(|p| {
                let mid = self.books.get(&p.ticker).and_then(|b| b.mid());
                let (mark, mtm, value) = self.marked(p);
                json!({"ticker": p.ticker, "yes_qty": p.yes_qty.to_f64(), "cash": p.cash.to_f64(), "fees": p.fees.to_f64(),
                       "n_fills": p.n_fills, "volume": p.volume.to_f64(), "mid": mid.map(|m| m.to_f64()),
                       "mark": mark, "mtm": mtm, "value": value})
            })
            .collect();
        let unreal = self.unrealized();
        let pos_value = self.positions_value();
        json!({
            "run_id": run_id, "mode": "LIVE", "strategy": strategy_name, "started_ms": started_ms, "updated_ms": self.now_ms,
            "initial_cash": self.initial_cash.to_f64(), "cash": self.cash.to_f64(), "free_cash": (self.cfg.max_notional - self.notional_at_risk()).max(0.0),
            "settled_pnl": self.cash.to_f64() - self.initial_cash.to_f64(), "unrealized": unreal, "equity": self.cash.to_f64() + pos_value,
            "n_fills": self.fills.len(), "settled_count": 0, "settled_wins": 0, "settled": [], "markets_seen": self.books.len(),
            "positions": positions,
            "open_orders": self.orders.iter().map(|(id, o)| json!({"id": id.0, "ticker": o.req.ticker, "action": format!("{:?}", o.req.action), "yes_px": o.req.yes_px.to_f64(), "qty": o.req.qty.to_f64(), "remaining": o.remaining.to_f64(), "ahead": 0, "tag": o.req.tag, "kalshi_id": o.kalshi_id})).collect::<Vec<_>>(),
            "books": self.books.iter().map(|(t, b)| (t.clone(), json!({"bid": b.best_bid().map(|(p, q)| [p.to_f64(), q.to_f64()]), "ask": b.best_ask().map(|(p, q)| [p.to_f64(), q.to_f64()]), "mid": b.mid().map(|m| m.to_f64()), "ts_ms": b.ts_ms}))).collect::<serde_json::Map<_, _>>(),
            "fills": [],
            "queue": {"rested": self.sent, "avg_ahead": 0, "reached_front": 0, "fills_at_price": 0, "fills_through": 0},
            "risk": {"max_notional": self.cfg.max_notional, "at_risk": self.notional_at_risk(), "max_loss": self.cfg.max_loss, "halted": self.halted, "rejected": self.rejected, "dry_run": self.cfg.dry_run},
            "strategy_state": strategy_state,
        })
    }
}

impl Context for KalshiExecutor {
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
        // strategies size off this: never let them see more than the risk budget
        Fp::from_f64((self.cfg.max_notional - self.notional_at_risk()).max(0.0).min(self.cash.to_f64()))
    }
    fn fee_model(&self, ticker: &str) -> FeeModel {
        self.fee_for(ticker)
    }
    fn submit(&mut self, req: OrderRequest) -> OrderId {
        let id = OrderId(self.next_id);
        self.next_id += 1;
        if self.halted {
            self.rejected += 1;
            return id;
        }
        let cost = match req.action {
            Action::Buy => req.qty.to_f64() * req.yes_px.to_f64(),
            Action::Sell => req.qty.to_f64() * (1.0 - req.yes_px.to_f64()),
        };
        if req.qty.to_f64() > self.cfg.max_order_qty
            || self.orders.len() >= self.cfg.max_open_orders
            || self.notional_at_risk() + cost > self.cfg.max_notional
            || cost > self.cash.to_f64()
        {
            warn!(ticker = %req.ticker, qty = %req.qty, px = %req.yes_px, at_risk = self.notional_at_risk(), "order REJECTED by risk limits");
            self.rejected += 1;
            return id;
        }
        self.orders.insert(
            id,
            LiveOrder {
                req: req.clone(),
                kalshi_id: None,
                remaining: req.qty,
                cancel_requested: false,
            },
        );
        self.sent += 1;
        let _ = self.cmd_tx.send(Cmd::Submit { id, req });
        id
    }
    fn cancel(&mut self, id: OrderId) {
        if let Some(o) = self.orders.get_mut(&id) {
            match &o.kalshi_id {
                Some(k) => {
                    let _ = self.cmd_tx.send(Cmd::Cancel { kalshi_id: k.clone() });
                    self.orders.remove(&id);
                }
                None => o.cancel_requested = true,
            }
        }
    }
    fn open_orders(&self, ticker: &str) -> Vec<(OrderId, OrderRequest)> {
        self.orders.iter().filter(|(_, o)| o.req.ticker == ticker).map(|(id, o)| (*id, o.req.clone())).collect()
    }
}

/// Convenience for the CLI: which outcome a settlement string means.
pub fn outcome_of(s: &str) -> Option<Outcome> {
    match s {
        "yes" => Some(Outcome::Yes),
        "no" => Some(Outcome::No),
        _ => None,
    }
}
