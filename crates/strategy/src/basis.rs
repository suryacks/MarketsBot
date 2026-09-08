//! Online estimate of the basis between our reference feed (e.g. Coinbase
//! last trade) and the index the contract settles on (CF Benchmarks BRTI).
//!
//! Each new market's strike *is* the 60-second average of the index just
//! before open, so at every open we get one clean observation:
//! basis = strike − mean(ref over [open−60s, open)). An EWMA of those keeps a
//! live estimate that is added to spot before pricing.

use std::collections::VecDeque;

#[derive(Debug, Clone)]
pub struct BasisEstimator {
    window_ms: i64,
    keep_ms: i64,
    refs: VecDeque<(i64, f64)>,
    lambda: f64,
    basis: Option<f64>,
    n: u64,
    pub last_sample: Option<f64>,
    sum: f64,
    sum_sq: f64,
}

impl BasisEstimator {
    pub fn new(window_secs: f64, lambda: f64) -> Self {
        let window_ms = (window_secs * 1000.0) as i64;
        Self {
            window_ms,
            keep_ms: window_ms * 3,
            refs: VecDeque::new(),
            lambda,
            basis: None,
            n: 0,
            last_sample: None,
            sum: 0.0,
            sum_sq: 0.0,
        }
    }

    pub fn on_ref(&mut self, ts_ms: i64, px: f64) {
        self.refs.push_back((ts_ms, px));
        while let Some(&(t, _)) = self.refs.front() {
            if ts_ms - t > self.keep_ms {
                self.refs.pop_front();
            } else {
                break;
            }
        }
    }

    /// Mean reference price over [open−window, open). Falls back to the most recent price
    /// before `open` (within `keep_ms`) when the feed is coarser than the window — minute
    /// bars give only one point per 60 s, which would otherwise yield no basis at all.
    pub fn ref_avg_before(&self, open_ts_ms: i64) -> Option<f64> {
        let lo = open_ts_ms - self.window_ms;
        let mut s = 0.0;
        let mut n = 0usize;
        for &(t, p) in self.refs.iter() {
            if t >= lo && t < open_ts_ms {
                s += p;
                n += 1;
            }
        }
        if n >= 2 {
            return Some(s / n as f64);
        }
        self.refs
            .iter()
            .filter(|(t, _)| *t < open_ts_ms && open_ts_ms - *t <= self.keep_ms)
            .next_back()
            .map(|(_, p)| *p)
    }

    /// Feed a new market's strike at its open. Returns the basis sample if computable.
    pub fn on_market_open(&mut self, open_ts_ms: i64, strike: f64) -> Option<f64> {
        let avg = self.ref_avg_before(open_ts_ms)?;
        let sample = strike - avg;
        // guard against garbage (a $2000 basis is a data problem, not a basis).
        // 3% covers the gold futures/spot basis (~1%) while still rejecting nonsense.
        if !sample.is_finite() || sample.abs() > strike * 0.03 {
            return None;
        }
        self.basis = Some(match self.basis {
            None => sample,
            Some(b) => self.lambda * b + (1.0 - self.lambda) * sample,
        });
        self.n += 1;
        self.last_sample = Some(sample);
        self.sum += sample;
        self.sum_sq += sample * sample;
        Some(sample)
    }

    pub fn basis(&self) -> f64 {
        self.basis.unwrap_or(0.0)
    }

    pub fn samples(&self) -> u64 {
        self.n
    }

    /// (mean, std) of all samples seen.
    pub fn stats(&self) -> (f64, f64) {
        if self.n == 0 {
            return (0.0, 0.0);
        }
        let n = self.n as f64;
        let mean = self.sum / n;
        let var = (self.sum_sq / n - mean * mean).max(0.0);
        (mean, var.sqrt())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn learns_constant_basis() {
        let mut b = BasisEstimator::new(60.0, 0.5);
        for t in 0..120 {
            b.on_ref(t * 1000, 100.0);
        }
        // strike is 100.3: index trades 0.3 above our feed
        assert!((b.on_market_open(120_000, 100.3).unwrap() - 0.3).abs() < 1e-9);
        for t in 120..240 {
            b.on_ref(t * 1000, 100.0);
        }
        b.on_market_open(240_000, 100.3);
        assert!((b.basis() - 0.3).abs() < 1e-9);
        assert_eq!(b.samples(), 2);
    }

    #[test]
    fn rejects_outliers_and_sparse_windows() {
        let mut b = BasisEstimator::new(60.0, 0.5);
        assert!(b.on_market_open(1000, 100.0).is_none());
        for t in 0..120 {
            b.on_ref(t * 1000, 100.0);
        }
        assert!(b.on_market_open(120_000, 150.0).is_none());
    }

    #[test]
    fn coarse_feed_still_yields_a_basis() {
        // one reference point per minute (Yahoo minute bars): the 60 s window holds a single
        // point, so the estimator must fall back to the last price before open.
        let mut b = BasisEstimator::new(60.0, 0.5);
        for m in 0..5 {
            b.on_ref(m * 60_000, 4440.0);
        }
        let s = b.on_market_open(4 * 60_000 + 30_000, 4398.0).expect("basis from coarse feed");
        assert!((s + 42.0).abs() < 1e-6, "{s}");
    }
}
