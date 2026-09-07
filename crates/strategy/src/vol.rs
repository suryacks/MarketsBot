//! Realized volatility from an irregularly-sampled price stream.
//!
//! Keeps an EWMA of per-second variance: v ← λ·v + (1−λ)·r²/Δt, but only
//! takes a sample every `sample_ms` (default 60 s). Sampling at tick frequency
//! is badly biased upward by bid/ask bounce and by uneven gaps (e.g. the
//! 1-second gap between a candle close and the next candle open), which is
//! exactly the kind of error that makes a fair-value model buy every cheap-looking
//! contract and lose. The *latest* price is still tracked on every tick.

pub const SECS_PER_YEAR: f64 = 365.0 * 86_400.0;

#[derive(Debug, Clone)]
pub struct RealizedVol {
    lambda: f64,
    var_per_sec: f64,
    sample_px: Option<f64>,
    sample_ts_ms: i64,
    sample_ms: i64,
    n: u64,
    floor_per_sec: f64,
    cap_per_sec: f64,
}

impl RealizedVol {
    pub fn new(lambda: f64, floor_annual: f64, cap_annual: f64, sample_ms: i64) -> Self {
        Self {
            lambda,
            var_per_sec: 0.0,
            sample_px: None,
            sample_ts_ms: 0,
            sample_ms: sample_ms.max(1),
            n: 0,
            floor_per_sec: floor_annual / SECS_PER_YEAR.sqrt(),
            cap_per_sec: cap_annual / SECS_PER_YEAR.sqrt(),
        }
    }

    pub fn update(&mut self, ts_ms: i64, px: f64) {
        if !(px > 0.0) {
            return;
        }
        match self.sample_px {
            None => {
                self.sample_px = Some(px);
                self.sample_ts_ms = ts_ms;
            }
            Some(prev) => {
                let dt_ms = ts_ms - self.sample_ts_ms;
                if dt_ms < self.sample_ms {
                    return;
                }
                let dt = dt_ms as f64 / 1000.0;
                let r = (px / prev).ln();
                let sample = r * r / dt;
                self.var_per_sec = if self.n == 0 {
                    sample
                } else {
                    self.lambda * self.var_per_sec + (1.0 - self.lambda) * sample
                };
                self.n += 1;
                self.sample_px = Some(px);
                self.sample_ts_ms = ts_ms;
            }
        }
    }

    /// Per-second sigma, clamped to [floor, cap].
    pub fn sigma_per_sec(&self) -> f64 {
        let s = self.var_per_sec.max(0.0).sqrt();
        s.clamp(self.floor_per_sec, self.cap_per_sec)
    }

    pub fn sigma_annual(&self) -> f64 {
        self.sigma_per_sec() * SECS_PER_YEAR.sqrt()
    }

    pub fn samples(&self) -> u64 {
        self.n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_price_hits_floor() {
        let mut v = RealizedVol::new(0.97, 0.25, 2.0, 1000);
        for i in 0..100 {
            v.update(i * 1000, 100.0);
        }
        assert!((v.sigma_annual() - 0.25).abs() < 1e-9);
    }

    #[test]
    fn recovers_known_vol_roughly() {
        // deterministic +/- alternating returns of size r each second => var = r^2
        let mut v = RealizedVol::new(0.99, 0.01, 10.0, 1000);
        let r: f64 = 1e-4; // per-second
        let mut px: f64 = 100.0;
        for i in 0..5000 {
            px *= if i % 2 == 0 { r.exp() } else { (-r).exp() };
            v.update(i * 1000, px);
        }
        assert!((v.sigma_per_sec() - r).abs() / r < 0.05);
    }

    #[test]
    fn ignores_sub_interval_ticks() {
        let mut v = RealizedVol::new(0.99, 0.0, 10.0, 60_000);
        // a huge bounce inside the interval must not register
        v.update(0, 100.0);
        v.update(500, 101.0);
        v.update(1_000, 100.0);
        assert_eq!(v.samples(), 0);
        v.update(60_000, 100.0);
        assert_eq!(v.samples(), 1);
        assert!(v.sigma_per_sec() < 1e-9);
    }
}
