//! Realized volatility from an irregularly-sampled price stream.
//! Keeps an EWMA of per-second variance: v ← λ·v + (1−λ)·r²/Δt.

pub const SECS_PER_YEAR: f64 = 365.0 * 86_400.0;

#[derive(Debug, Clone)]
pub struct RealizedVol {
    lambda: f64,
    var_per_sec: f64,
    last_px: Option<f64>,
    last_ts_ms: i64,
    /// Ignore samples closer together than this (microstructure noise).
    min_dt_ms: i64,
    n: u64,
    floor_per_sec: f64,
    cap_per_sec: f64,
}

impl RealizedVol {
    pub fn new(lambda: f64, floor_annual: f64, cap_annual: f64, min_dt_ms: i64) -> Self {
        Self {
            lambda,
            var_per_sec: 0.0,
            last_px: None,
            last_ts_ms: 0,
            min_dt_ms,
            n: 0,
            floor_per_sec: floor_annual / SECS_PER_YEAR.sqrt(),
            cap_per_sec: cap_annual / SECS_PER_YEAR.sqrt(),
        }
    }

    pub fn update(&mut self, ts_ms: i64, px: f64) {
        if !(px > 0.0) {
            return;
        }
        if let Some(prev) = self.last_px {
            let dt_ms = ts_ms - self.last_ts_ms;
            if dt_ms < self.min_dt_ms {
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
        }
        self.last_px = Some(px);
        self.last_ts_ms = ts_ms;
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

    pub fn last(&self) -> Option<(i64, f64)> {
        self.last_px.map(|p| (self.last_ts_ms, p))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_price_hits_floor() {
        let mut v = RealizedVol::new(0.97, 0.25, 2.0, 100);
        for i in 0..100 {
            v.update(i * 1000, 100.0);
        }
        assert!((v.sigma_annual() - 0.25).abs() < 1e-9);
    }

    #[test]
    fn recovers_known_vol_roughly() {
        // deterministic +/- alternating returns of size r each second => var = r^2
        let mut v = RealizedVol::new(0.99, 0.01, 10.0, 100);
        let r: f64 = 1e-4; // per-second
        let mut px: f64 = 100.0;
        for i in 0..5000 {
            px *= if i % 2 == 0 { r.exp() } else { (-r).exp() };
            v.update(i * 1000, px);
        }
        assert!((v.sigma_per_sec() - r).abs() / r < 0.05);
    }
}
