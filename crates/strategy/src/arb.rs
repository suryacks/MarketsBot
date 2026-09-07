//! Structural arbitrage detectors. Pure functions over prices so they can be
//! unit-tested and reused by a scanner, the backtester or a live engine.

use mb_core::{FeeModel, Fp};

#[derive(Debug, Clone, PartialEq)]
pub enum ArbSignal {
    /// Buy YES on every leg: Σ asks < 1 (one leg must pay $1).
    BuyAllYes { cost: f64, fees: f64, profit: f64 },
    /// Buy NO on every leg: Σ (1 − bid) < N − 1 (exactly N−1 legs pay $1).
    BuyAllNo { cost: f64, fees: f64, profit: f64 },
}

/// Mutually-exclusive, exhaustive outcome set (e.g. price ranges, "who wins").
/// `legs` = (best_bid, best_ask) in YES terms for each outcome.
pub fn multi_outcome_arb(legs: &[(Option<Fp>, Option<Fp>)], fee: &FeeModel, min_profit: f64) -> Option<ArbSignal> {
    let n = legs.len();
    if n < 2 {
        return None;
    }
    // Buy all YES
    if legs.iter().all(|(_, a)| a.is_some()) {
        let cost: f64 = legs.iter().map(|(_, a)| a.unwrap().to_f64()).sum();
        let fees: f64 = legs.iter().map(|(_, a)| fee.fee_per_contract(a.unwrap(), false)).sum();
        let profit = 1.0 - cost - fees;
        if profit > min_profit {
            return Some(ArbSignal::BuyAllYes { cost, fees, profit });
        }
    }
    // Buy all NO: NO ask = 1 − YES bid
    if legs.iter().all(|(b, _)| b.is_some()) {
        let cost: f64 = legs.iter().map(|(b, _)| 1.0 - b.unwrap().to_f64()).sum();
        let fees: f64 = legs.iter().map(|(b, _)| fee.fee_per_contract(b.unwrap(), false)).sum();
        let profit = (n as f64 - 1.0) - cost - fees;
        if profit > min_profit {
            return Some(ArbSignal::BuyAllNo { cost, fees, profit });
        }
    }
    None
}

#[derive(Debug, Clone, PartialEq)]
pub struct CrossVenueArb {
    /// Buy YES here…
    pub buy_yes_at: f64,
    /// …and NO there (expressed as the other venue's YES bid we sell into).
    pub sell_yes_at: f64,
    pub fees: f64,
    pub profit: f64,
}

/// Same binary event listed on two venues. Locks in `profit` per contract if
/// we can buy YES on A at `a_ask` and sell YES (buy NO) on B at `b_bid`.
pub fn cross_venue_arb(a_ask: Fp, a_fee: &FeeModel, b_bid: Fp, b_fee: &FeeModel, min_profit: f64) -> Option<CrossVenueArb> {
    let fees = a_fee.fee_per_contract(a_ask, false) + b_fee.fee_per_contract(b_bid, false);
    let profit = b_bid.to_f64() - a_ask.to_f64() - fees;
    if profit > min_profit {
        Some(CrossVenueArb {
            buy_yes_at: a_ask.to_f64(),
            sell_yes_at: b_bid.to_f64(),
            fees,
            profit,
        })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp(s: &str) -> Fp {
        Fp::parse(s).unwrap()
    }

    #[test]
    fn detects_buy_all_yes() {
        let fee = FeeModel::None;
        let legs = vec![(Some(fp("0.30")), Some(fp("0.31"))), (Some(fp("0.30")), Some(fp("0.31"))), (Some(fp("0.30")), Some(fp("0.31")))];
        match multi_outcome_arb(&legs, &fee, 0.0) {
            Some(ArbSignal::BuyAllYes { profit, .. }) => assert!((profit - 0.07).abs() < 1e-9),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn fees_kill_thin_arb() {
        let fee = FeeModel::kalshi_default();
        let legs = vec![(Some(fp("0.49")), Some(fp("0.50"))), (Some(fp("0.48")), Some(fp("0.49")))];
        // cost 0.99, fees ~0.0175*2 -> negative
        assert!(multi_outcome_arb(&legs, &fee, 0.0).is_none());
    }

    #[test]
    fn detects_buy_all_no() {
        let fee = FeeModel::None;
        // bids sum to 1.10 across 2 legs -> NO cost = 2 - 1.10 = 0.90 < 1
        let legs = vec![(Some(fp("0.55")), Some(fp("0.60"))), (Some(fp("0.55")), Some(fp("0.60")))];
        match multi_outcome_arb(&legs, &fee, 0.0) {
            Some(ArbSignal::BuyAllNo { profit, .. }) => assert!((profit - 0.10).abs() < 1e-9),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn cross_venue() {
        let k = FeeModel::kalshi_default();
        let p = FeeModel::polymarket(0.04);
        let sig = cross_venue_arb(fp("0.40"), &k, fp("0.46"), &p, 0.01).unwrap();
        assert!(sig.profit > 0.02 && sig.profit < 0.06);
        assert!(cross_venue_arb(fp("0.45"), &k, fp("0.46"), &p, 0.0).is_none());
    }
}
