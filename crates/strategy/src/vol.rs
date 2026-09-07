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

/// Rolling *median* of market-implied per-second vol (from
/// `fair_value::implied_sigma`). Individual binary prints imply wild vols
/// whenever the market lags spot, so a mean/EWMA is useless; the median over
/// the last `window` accepted prints is robust to that.
#[derive(Debug, Clone)]
pub struct ImpliedVol {
    window: usize,
    buf: std::collections::VecDeque<f64>,
    n: u64,
    max_per_sec: f64,
}

impl ImpliedVol {
    /// `lambda` is kept for config compatibility and mapped to a window: 0.98 → 50, 0.99 → 100.
    pub fn new(lambda: f64) -> Self {
        let window = ((1.0 / (1.0 - lambda.clamp(0.5, 0.999))).round() as usize).clamp(10, 2000);
        Self {
            window,
            buf: std::collections::VecDeque::with_capacity(window + 1),
            n: 0,
            max_per_sec: 3.0 / SECS_PER_YEAR.sqrt(), // 300% annualized: anything above is a lag artifact
        }
    }
    pub fn update(&mut self, sigma_per_sec: f64) {
        if !(sigma_per_sec > 0.0) || !sigma_per_sec.is_finite() || sigma_per_sec > self.max_per_sec {
            return;
        }
        self.buf.push_back(sigma_per_sec);
        if self.buf.len() > self.window {
            self.buf.pop_front();
        }
        self.n += 1;
    }
    pub fn sigma_per_sec(&self) -> Option<f64> {
        if self.buf.len() < 5 {
            return None;
        }
        let mut v: Vec<f64> = self.buf.iter().copied().collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        Some(v[v.len() / 2])
    }
    pub fn samples(&self) -> u64 {
        self.n
    }
}

/// Which vol feeds the pricer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolSource {
    Realized,
    Implied,
    /// max(realized, implied) — never price tighter than the market
    Max,
    /// geometric mean of the two
    Mean,
}

impl VolSource {
    pub fn parse(s: &str) -> VolSource {
        match s {
            "implied" => VolSource::Implied,
            "max" => VolSource::Max,
            "mean" => VolSource::Mean,
            _ => VolSource::Realized,
        }
    }
}

/// Realized + implied vol behind one `sigma_per_sec()`; shared by the strategy and `calibrate`.
#[derive(Debug, Clone)]
pub struct VolModel {
    pub realized: RealizedVol,
    pub implied: ImpliedVol,
    pub source: VolSource,
    floor_per_sec: f64,
    cap_per_sec: f64,
}

impl VolModel {
    pub fn new(realized: RealizedVol, implied: ImpliedVol, source: VolSource, floor_annual: f64, cap_annual: f64) -> Self {
        Self {
            realized,
            implied,
            source,
            floor_per_sec: floor_annual / SECS_PER_YEAR.sqrt(),
            cap_per_sec: cap_annual / SECS_PER_YEAR.sqrt(),
        }
    }
    pub fn on_ref(&mut self, ts_ms: i64, px: f64) {
        self.realized.update(ts_ms, px);
    }
    /// Feed a market print: (spot already basis-adjusted, strike, yes price, seconds to close).
    pub fn on_print(&mut self, spot: f64, strike: f64, px: f64, tau_secs: f64, avg_window_secs: f64) {
        if let Some(s) = crate::fair_value::implied_sigma(spot, strike, px, tau_secs, avg_window_secs) {
            self.implied.update(s);
        }
    }
    pub fn sigma_per_sec(&self) -> f64 {
        let r = self.realized.sigma_per_sec();
        let s = match (self.source, self.implied.sigma_per_sec()) {
            (VolSource::Realized, _) | (_, None) => r,
            (VolSource::Implied, Some(i)) => i,
            (VolSource::Max, Some(i)) => r.max(i),
            (VolSource::Mean, Some(i)) => (r * i).sqrt(),
        };
        s.clamp(self.floor_per_sec, self.cap_per_sec)
    }
    pub fn sigma_annual(&self) -> f64 {
        self.sigma_per_sec() * SECS_PER_YEAR.sqrt()
    }
    pub fn implied_annual(&self) -> Option<f64> {
        self.implied.sigma_per_sec().map(|s| s * SECS_PER_YEAR.sqrt())
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
