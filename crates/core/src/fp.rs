//! 4-decimal fixed-point number. Used for both prices (dollars, 0..=1 for a
//! binary contract) and quantities (contracts). No floats on the hot path.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::iter::Sum;
use std::ops::{Add, AddAssign, Neg, Sub, SubAssign};

pub const SCALE: i64 = 10_000;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Fp(pub i64);

#[derive(Debug, thiserror::Error)]
#[error("invalid fixed-point literal: {0:?}")]
pub struct FpParseError(pub String);

impl Fp {
    pub const ZERO: Fp = Fp(0);
    pub const ONE: Fp = Fp(SCALE);
    pub const CENT: Fp = Fp(100);
    pub const TICK: Fp = Fp(10); // 0.001 — finest price level on either venue

    pub const fn raw(v: i64) -> Fp {
        Fp(v)
    }
    pub fn from_f64(v: f64) -> Fp {
        Fp((v * SCALE as f64).round() as i64)
    }
    pub fn from_int(n: i64) -> Fp {
        Fp(n * SCALE)
    }
    pub fn to_f64(self) -> f64 {
        self.0 as f64 / SCALE as f64
    }
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }
    pub fn is_positive(self) -> bool {
        self.0 > 0
    }
    pub fn abs(self) -> Fp {
        Fp(self.0.abs())
    }
    pub fn min(self, o: Fp) -> Fp {
        if self <= o { self } else { o }
    }
    pub fn max(self, o: Fp) -> Fp {
        if self >= o { self } else { o }
    }
    /// `1 - p` for a binary contract price.
    pub fn complement(self) -> Fp {
        Fp(SCALE - self.0)
    }
    /// Fixed-point multiply (price × qty → dollars).
    pub fn mul(self, o: Fp) -> Fp {
        Fp(((self.0 as i128 * o.0 as i128) / SCALE as i128) as i64)
    }
    pub fn div(self, o: Fp) -> Fp {
        Fp(((self.0 as i128 * SCALE as i128) / o.0 as i128) as i64)
    }
    pub fn scale_f64(self, k: f64) -> Fp {
        Fp((self.0 as f64 * k).round() as i64)
    }
    /// Round up to the next cent (Kalshi fee rounding).
    pub fn ceil_cent(self) -> Fp {
        let r = self.0.rem_euclid(100);
        if r == 0 { self } else { Fp(self.0 + (100 - r)) }
    }
    /// Round to the nearest multiple of `tick` (toward the given direction).
    pub fn round_down_to(self, tick: Fp) -> Fp {
        Fp(self.0.div_euclid(tick.0) * tick.0)
    }
    pub fn round_up_to(self, tick: Fp) -> Fp {
        let r = self.0.rem_euclid(tick.0);
        if r == 0 { self } else { Fp(self.0 + tick.0 - r) }
    }

    /// Parse "0.5600", "31.68", "-54.00", "12" without going through f64.
    pub fn parse(s: &str) -> Result<Fp, FpParseError> {
        let raw = s.trim();
        let (neg, s) = match raw.strip_prefix('-') {
            Some(r) => (true, r),
            None => (false, raw),
        };
        let (int_part, frac_part) = match s.split_once('.') {
            Some((i, f)) => (i, f),
            None => (s, ""),
        };
        let int: i64 = if int_part.is_empty() {
            0
        } else {
            int_part.parse().map_err(|_| FpParseError(raw.to_string()))?
        };
        let fb = frac_part.as_bytes();
        let mut frac: i64 = 0;
        for i in 0..4 {
            let d = match fb.get(i) {
                Some(c) if c.is_ascii_digit() => (c - b'0') as i64,
                Some(_) => return Err(FpParseError(raw.to_string())),
                None => 0,
            };
            frac = frac * 10 + d;
        }
        let v = int * SCALE + frac;
        Ok(Fp(if neg { -v } else { v }))
    }

    /// Format with a fixed number of decimals (Kalshi wants e.g. "0.5600" / "10.00").
    pub fn fmt_dec(self, decimals: u32) -> String {
        let neg = self.0 < 0;
        let v = self.0.abs();
        let int = v / SCALE;
        let frac = v % SCALE;
        let s = if decimals == 0 {
            format!("{int}")
        } else {
            let f = format!("{frac:04}");
            format!("{int}.{}", &f[..decimals.min(4) as usize])
        };
        if neg { format!("-{s}") } else { s }
    }
}

impl fmt::Display for Fp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.fmt_dec(4))
    }
}
impl fmt::Debug for Fp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Fp({})", self.fmt_dec(4))
    }
}

impl Add for Fp {
    type Output = Fp;
    fn add(self, o: Fp) -> Fp {
        Fp(self.0 + o.0)
    }
}
impl Sub for Fp {
    type Output = Fp;
    fn sub(self, o: Fp) -> Fp {
        Fp(self.0 - o.0)
    }
}
impl Neg for Fp {
    type Output = Fp;
    fn neg(self) -> Fp {
        Fp(-self.0)
    }
}
impl AddAssign for Fp {
    fn add_assign(&mut self, o: Fp) {
        self.0 += o.0;
    }
}
impl SubAssign for Fp {
    fn sub_assign(&mut self, o: Fp) {
        self.0 -= o.0;
    }
}
impl Sum for Fp {
    fn sum<I: Iterator<Item = Fp>>(iter: I) -> Fp {
        iter.fold(Fp::ZERO, |a, b| a + b)
    }
}

impl Serialize for Fp {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.fmt_dec(4))
    }
}

impl<'de> Deserialize<'de> for Fp {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Fp, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = Fp;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a decimal string or number")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Fp, E> {
                Fp::parse(v).map_err(E::custom)
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Fp, E> {
                Ok(Fp::from_f64(v))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Fp, E> {
                Ok(Fp::from_int(v))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Fp, E> {
                Ok(Fp::from_int(v as i64))
            }
        }
        d.deserialize_any(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_roundtrip() {
        assert_eq!(Fp::parse("0.5600").unwrap(), Fp(5600));
        assert_eq!(Fp::parse("31.68").unwrap(), Fp(316_800));
        assert_eq!(Fp::parse("-54.00").unwrap(), Fp(-540_000));
        assert_eq!(Fp::parse("12").unwrap(), Fp(120_000));
        assert_eq!(Fp::parse("0.001").unwrap(), Fp(10));
        assert_eq!(Fp::parse("219.217767").unwrap(), Fp(2_192_177)); // truncates
        assert_eq!(Fp(5600).fmt_dec(4), "0.5600");
        assert_eq!(Fp(100_000).fmt_dec(2), "10.00");
        assert_eq!(Fp(-540_000).to_string(), "-54.0000");
    }

    #[test]
    fn arithmetic() {
        assert_eq!(Fp::parse("0.5").unwrap().mul(Fp::from_int(10)), Fp::from_int(5));
        assert_eq!(Fp(5600).complement(), Fp(4400));
        assert_eq!(Fp(101).ceil_cent(), Fp(200));
        assert_eq!(Fp(200).ceil_cent(), Fp(200));
        assert_eq!(Fp(5555).round_down_to(Fp::CENT), Fp(5500));
        assert_eq!(Fp(5555).round_up_to(Fp::CENT), Fp(5600));
    }
}
