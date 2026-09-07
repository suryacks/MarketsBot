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

/// Inverse standard normal CDF (Acklam's rational approximation, |err| < 1.2e-9).
pub fn norm_ppf(p: f64) -> f64 {
    const A: [f64; 6] = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00];
    const B: [f64; 5] = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01, -1.328068155288572e+01];
    const C: [f64; 6] = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00];
    const D: [f64; 4] = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00];
    let p = p.clamp(1e-12, 1.0 - 1e-12);
    let (plow, phigh) = (0.02425, 1.0 - 0.02425);
    if p < plow {
        let q = (-2.0 * p.ln()).sqrt();
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5]) / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else if p <= phigh {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        let q = (-2.0 * (1.0 - p).ln()).sqrt();
        -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5]) / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    }
}

/// Per-second vol implied by a market price `px` for P(S_T ≥ K), inverting
/// `prob_above`. Returns None when the print carries no vol information
/// (near ATM, extreme prices, or inconsistent sign).
pub fn implied_sigma(spot: f64, strike: f64, px: f64, tau_secs: f64, avg_window_secs: f64) -> Option<f64> {
    let te = effective_tau(tau_secs, avg_window_secs);
    if te <= 0.0 || !(spot > 0.0) || !(strike > 0.0) || !(0.08..=0.92).contains(&px) {
        return None;
    }
    let m = (spot / strike).ln();
    let z = norm_ppf(px);
    // ATM prints (or prints on the wrong side of the strike) don't identify sigma;
    // near-ATM prints identify it so badly that a 1-tick move flips the answer.
    if z.abs() < 0.5 || m.abs() < 1e-4 || (m > 0.0) != (z > 0.0) {
        return None;
    }
    // m = z·σ√te + σ²te/2  →  (te/2)σ² + (z√te)σ − m = 0
    let a = te / 2.0;
    let b = z * te.sqrt();
    let disc = b * b + 4.0 * a * m;
    if disc < 0.0 {
        return None;
    }
    let s = (-b + disc.sqrt()) / (2.0 * a);
    if s.is_finite() && s > 0.0 { Some(s) } else { None }
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
    fn ppf_inverts_cdf() {
        for &p in &[0.01, 0.1, 0.3, 0.5, 0.7, 0.9, 0.99] {
            assert!((norm_cdf(norm_ppf(p)) - p).abs() < 1e-8, "p={p}");
        }
    }

    #[test]
    fn implied_sigma_roundtrips() {
        let sig = 0.4 / (365.0f64 * 86400.0).sqrt();
        let px = prob_above(80_150.0, 80_000.0, sig, 700.0, 60.0);
        let back = implied_sigma(80_150.0, 80_000.0, px, 700.0, 60.0).unwrap();
        assert!((back - sig).abs() / sig < 1e-4, "{back} vs {sig}");
        assert!(implied_sigma(80_000.0, 80_000.0, 0.5, 700.0, 60.0).is_none()); // ATM
        assert!(implied_sigma(80_100.0, 80_000.0, 0.3, 700.0, 60.0).is_none()); // wrong side
    }

    #[test]
    fn kelly() {
        assert!((kelly_fraction_buy(0.6, 0.5) - 0.2).abs() < 1e-12);
        assert_eq!(kelly_fraction_buy(0.4, 0.5), 0.0);
    }
}
