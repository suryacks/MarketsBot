use crate::fp::Fp;

/// Kalshi's published general taker rate: fee = 0.07 × C × P × (1 − P), rounded
/// up to the next cent per fill. Series carry a `fee_multiplier` on top.
pub const KALSHI_BASE_TAKER_RATE: f64 = 0.07;
/// Maker rate for series with `fee_type = quadratic_with_maker_fees`.
pub const KALSHI_BASE_MAKER_RATE: f64 = 0.0175;

#[derive(Clone, Debug, PartialEq)]
pub enum FeeModel {
    /// fee = rate × C × P × (1 − P)  (both Kalshi and Polymarket use this shape)
    Quadratic {
        taker_rate: f64,
        maker_rate: f64,
        round_up_cent: bool,
    },
    Flat {
        per_contract: Fp,
    },
    None,
}

impl FeeModel {
    /// Build from a Kalshi series' `fee_type` / `fee_multiplier`.
    pub fn kalshi(fee_type: &str, multiplier: f64) -> FeeModel {
        let m = if multiplier <= 0.0 { 1.0 } else { multiplier };
        match fee_type {
            "quadratic" => FeeModel::Quadratic {
                taker_rate: KALSHI_BASE_TAKER_RATE * m,
                maker_rate: 0.0,
                round_up_cent: true,
            },
            "quadratic_with_maker_fees" | "quadratic_with_combo_maker_fees" => FeeModel::Quadratic {
                taker_rate: KALSHI_BASE_TAKER_RATE * m,
                maker_rate: KALSHI_BASE_MAKER_RATE * m,
                round_up_cent: true,
            },
            // TODO: confirm flat-fee series rates against the Kalshi fee schedule.
            "flat" => FeeModel::Flat {
                per_contract: Fp::from_f64(0.01 * m),
            },
            _ => FeeModel::Quadratic {
                taker_rate: KALSHI_BASE_TAKER_RATE * m,
                maker_rate: 0.0,
                round_up_cent: true,
            },
        }
    }

    pub fn kalshi_default() -> FeeModel {
        FeeModel::kalshi("quadratic", 1.0)
    }

    /// Polymarket: taker-only, rate per market category (0.04–0.07, 0 for geopolitics).
    pub fn polymarket(rate: f64) -> FeeModel {
        if rate <= 0.0 {
            FeeModel::None
        } else {
            FeeModel::Quadratic {
                taker_rate: rate,
                maker_rate: 0.0,
                round_up_cent: false,
            }
        }
    }

    /// Fee in dollars for a fill of `qty` contracts at YES price `px`.
    pub fn fee(&self, px: Fp, qty: Fp, is_maker: bool) -> Fp {
        match self {
            FeeModel::None => Fp::ZERO,
            FeeModel::Flat { per_contract } => per_contract.mul(qty),
            FeeModel::Quadratic {
                taker_rate,
                maker_rate,
                round_up_cent,
            } => {
                let rate = if is_maker { *maker_rate } else { *taker_rate };
                if rate <= 0.0 {
                    return Fp::ZERO;
                }
                let p = px.to_f64();
                let raw = rate * qty.to_f64() * p * (1.0 - p);
                let f = Fp::from_f64(raw);
                if *round_up_cent { f.ceil_cent() } else { f }
            }
        }
    }

    /// Approximate fee per single contract (used for edge thresholds).
    pub fn fee_per_contract(&self, px: Fp, is_maker: bool) -> f64 {
        match self {
            FeeModel::None => 0.0,
            FeeModel::Flat { per_contract } => per_contract.to_f64(),
            FeeModel::Quadratic {
                taker_rate,
                maker_rate,
                ..
            } => {
                let rate = if is_maker { *maker_rate } else { *taker_rate };
                let p = px.to_f64();
                rate * p * (1.0 - p)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kalshi_taker_fee_rounds_up() {
        let m = FeeModel::kalshi_default();
        // 100 contracts at 0.50: 0.07*100*0.25 = $1.75
        assert_eq!(m.fee(Fp::parse("0.5").unwrap(), Fp::from_int(100), false), Fp::parse("1.75").unwrap());
        // 1 contract at 0.50: 0.0175 -> rounds up to 0.02
        assert_eq!(m.fee(Fp::parse("0.5").unwrap(), Fp::from_int(1), false), Fp::parse("0.02").unwrap());
        // maker pays nothing on plain quadratic
        assert_eq!(m.fee(Fp::parse("0.5").unwrap(), Fp::from_int(100), true), Fp::ZERO);
    }

    #[test]
    fn polymarket_crypto_fee() {
        let m = FeeModel::polymarket(0.07);
        assert_eq!(m.fee(Fp::parse("0.5").unwrap(), Fp::from_int(100), false), Fp::parse("1.75").unwrap());
        assert_eq!(FeeModel::polymarket(0.0), FeeModel::None);
    }
}
