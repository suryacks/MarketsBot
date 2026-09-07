use crate::fp::Fp;
use crate::types::BookSide;
use std::collections::BTreeMap;

/// Price-level orderbook in YES terms. Bids are buyers of YES, asks are sellers
/// of YES (== buyers of NO at 1 - price).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Orderbook {
    pub bids: BTreeMap<Fp, Fp>,
    pub asks: BTreeMap<Fp, Fp>,
    pub ts_ms: i64,
    pub seq: i64,
}

/// Result of walking one side of the book for a given quantity.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Sweep {
    pub filled: Fp,
    /// Sum of px*qty over the levels hit.
    pub cost: Fp,
    pub levels: Vec<(Fp, Fp)>,
}

impl Sweep {
    pub fn avg_px(&self) -> Option<Fp> {
        if self.filled.is_zero() { None } else { Some(self.cost.div(self.filled)) }
    }
}

impl Orderbook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    pub fn best_bid(&self) -> Option<(Fp, Fp)> {
        self.bids.iter().next_back().map(|(p, q)| (*p, *q))
    }

    pub fn best_ask(&self) -> Option<(Fp, Fp)> {
        self.asks.iter().next().map(|(p, q)| (*p, *q))
    }

    pub fn mid(&self) -> Option<Fp> {
        match (self.best_bid(), self.best_ask()) {
            (Some((b, _)), Some((a, _))) => Some(Fp((b.0 + a.0) / 2)),
            _ => None,
        }
    }

    pub fn spread(&self) -> Option<Fp> {
        match (self.best_bid(), self.best_ask()) {
            (Some((b, _)), Some((a, _))) => Some(a - b),
            _ => None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    pub fn set_level(&mut self, side: BookSide, px: Fp, qty: Fp) {
        let m = match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        };
        if qty.0 <= 0 {
            m.remove(&px);
        } else {
            m.insert(px, qty);
        }
    }

    pub fn apply_delta(&mut self, side: BookSide, px: Fp, delta: Fp) {
        let m = match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        };
        let cur = m.get(&px).copied().unwrap_or(Fp::ZERO);
        let new = cur + delta;
        if new.0 <= 0 {
            m.remove(&px);
        } else {
            m.insert(px, new);
        }
    }

    pub fn replace(&mut self, bids: &[(Fp, Fp)], asks: &[(Fp, Fp)], ts_ms: i64, seq: i64) {
        self.clear();
        for (p, q) in bids {
            if q.is_positive() {
                self.bids.insert(*p, *q);
            }
        }
        for (p, q) in asks {
            if q.is_positive() {
                self.asks.insert(*p, *q);
            }
        }
        self.ts_ms = ts_ms;
        self.seq = seq;
    }

    /// Build from Kalshi's representation: YES bids and NO bids. A NO bid at
    /// price p with size s is a YES ask at 1-p with size s.
    pub fn from_kalshi(yes_bids: &[(Fp, Fp)], no_bids: &[(Fp, Fp)], ts_ms: i64, seq: i64) -> Self {
        let asks: Vec<(Fp, Fp)> = no_bids.iter().map(|(p, q)| (p.complement(), *q)).collect();
        let mut ob = Orderbook::new();
        ob.replace(yes_bids, &asks, ts_ms, seq);
        ob
    }

    /// Walk the book as an aggressor. `BookSide::Ask` = we are buying YES and
    /// consume asks up to `limit_px`; `BookSide::Bid` = we are selling YES and
    /// consume bids down to `limit_px`.
    pub fn sweep(&self, side: BookSide, qty: Fp, limit_px: Option<Fp>) -> Sweep {
        let mut out = Sweep::default();
        let mut remaining = qty;
        match side {
            BookSide::Ask => {
                for (p, q) in self.asks.iter() {
                    if remaining.0 <= 0 {
                        break;
                    }
                    if let Some(l) = limit_px
                        && *p > l
                    {
                        break;
                    }
                    let take = remaining.min(*q);
                    out.levels.push((*p, take));
                    out.cost += p.mul(take);
                    out.filled += take;
                    remaining -= take;
                }
            }
            BookSide::Bid => {
                for (p, q) in self.bids.iter().rev() {
                    if remaining.0 <= 0 {
                        break;
                    }
                    if let Some(l) = limit_px
                        && *p < l
                    {
                        break;
                    }
                    let take = remaining.min(*q);
                    out.levels.push((*p, take));
                    out.cost += p.mul(take);
                    out.filled += take;
                    remaining -= take;
                }
            }
        }
        out
    }

    pub fn depth(&self, side: BookSide, n: usize) -> Vec<(Fp, Fp)> {
        match side {
            BookSide::Bid => self.bids.iter().rev().take(n).map(|(p, q)| (*p, *q)).collect(),
            BookSide::Ask => self.asks.iter().take(n).map(|(p, q)| (*p, *q)).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp(s: &str) -> Fp {
        Fp::parse(s).unwrap()
    }

    #[test]
    fn kalshi_no_bids_become_yes_asks() {
        let ob = Orderbook::from_kalshi(
            &[(fp("0.15"), fp("100")), (fp("0.17"), fp("1"))],
            &[(fp("0.82"), fp("50")), (fp("0.83"), fp("20"))],
            0,
            0,
        );
        assert_eq!(ob.best_bid(), Some((fp("0.17"), fp("1"))));
        assert_eq!(ob.best_ask(), Some((fp("0.17"), fp("20"))));
        assert_eq!(ob.spread(), Some(Fp::ZERO));
        assert_eq!(ob.asks.get(&fp("0.18")), Some(&fp("50")));
    }

    #[test]
    fn sweep_walks_levels_and_respects_limit() {
        let mut ob = Orderbook::new();
        ob.set_level(BookSide::Ask, fp("0.50"), fp("10"));
        ob.set_level(BookSide::Ask, fp("0.52"), fp("10"));
        let s = ob.sweep(BookSide::Ask, fp("15"), Some(fp("0.51")));
        assert_eq!(s.filled, fp("10"));
        assert_eq!(s.cost, fp("5"));
        let s = ob.sweep(BookSide::Ask, fp("15"), None);
        assert_eq!(s.filled, fp("15"));
        assert_eq!(s.cost, fp("7.6"));
        assert_eq!(s.avg_px(), Some(Fp::parse("0.506666").unwrap()));
    }

    #[test]
    fn delta_removes_empty_levels() {
        let mut ob = Orderbook::new();
        ob.apply_delta(BookSide::Bid, fp("0.4"), fp("5"));
        ob.apply_delta(BookSide::Bid, fp("0.4"), fp("-5"));
        assert!(ob.bids.is_empty());
    }
}
