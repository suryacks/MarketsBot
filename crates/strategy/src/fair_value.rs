//! Pricing a short-dated binary on a price index.
//!
//! Kalshi's KX*15M contracts pay $1 if the 60-second average of the index just
//! before close is ≥ the strike (which is itself the 60-second average just
//! before open). With τ seconds to close and per-second vol σ under a driftless
//! lognormal, P(YES) = Φ( (ln(S/K) − σ²τ_eff/2) / (σ·√τ_eff) ), where τ_eff
//! accounts for the settlement being an average rather than a point:
//! Var(mean of BM over the last w seconds) = σ²·(τ − w + w/3).

/// Standard normal CDF.
pub fn norm_cdf(x: f64) -> f64 {
    0.5 * libm::erfc(-x / std::f64::consts::SQRT_2)
}

/// Effective variance horizon in seconds for a `w`-second average settlement.
pub fn effective_tau(tau_secs: f64, avg_window_secs: f64) -> f64 {
    if tau_secs <= 0.0 {
        return 0.0;
    }
    if tau_secs <= avg_window_secs {
        // Inside the averaging window part of the outcome is already fixed;
        // approximate by the remaining fraction (the strategy shouldn't be trading here).
        return tau_secs / 3.0;
    }
    (tau_secs - avg_window_secs) + avg_window_secs / 3.0
}

/// Probability that the settlement value is ≥ `strike`.
pub fn prob_above(spot: f64, strike: f64, sigma_per_sec: f64, tau_secs: f64, avg_window_secs: f64) -> f64 {
    if !(spot > 0.0) || !(strike > 0.0) {
        return 0.5;
    }
    let te = effective_tau(tau_secs, avg_window_secs);
    if te <= 0.0 || sigma_per_sec <= 0.0 {
        return if spot >= strike { 1.0 } else { 0.0 };
    }
    let sd = sigma_per_sec * te.sqrt();
    let d = ((spot / strike).ln() - 0.5 * sd * sd) / sd;
    norm_cdf(d)
}

/// Fractional-Kelly stake (fraction of bankroll) for buying a binary at price
/// `px` when the true probability is `p`. Returns 0 when there is no edge.
pub fn kelly_fraction_buy(p: f64, px: f64) -> f64 {
    if px <= 0.0 || px >= 1.0 || p <= px {
        return 0.0;
    }
    (p - px) / (1.0 - px)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cdf_sanity() {
        assert!((norm_cdf(0.0) - 0.5).abs() < 1e-12);
        assert!((norm_cdf(1.96) - 0.975).abs() < 1e-3);
        assert!((norm_cdf(-1.96) - 0.025).abs() < 1e-3);
    }

    #[test]
    fn atm_is_half_and_moves_with_spot() {
        let sig = 0.5 / (365.0f64 * 86400.0).sqrt();
        let atm = prob_above(80_000.0, 80_000.0, sig, 600.0, 60.0);
        assert!((atm - 0.5).abs() < 0.01);
        let up = prob_above(80_200.0, 80_000.0, sig, 600.0, 60.0);
        let dn = prob_above(79_800.0, 80_000.0, sig, 600.0, 60.0);
        assert!(up > 0.7 && dn < 0.3);
        // near expiry the option is nearly digital
        assert!(prob_above(80_050.0, 80_000.0, sig, 5.0, 60.0) > 0.9);
    }

    #[test]
    fn kelly() {
        assert!((kelly_fraction_buy(0.6, 0.5) - 0.2).abs() < 1e-12);
        assert_eq!(kelly_fraction_buy(0.4, 0.5), 0.0);
    }
}
