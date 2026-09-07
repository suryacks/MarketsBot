//! Kalshi request signing: `timestamp_ms + METHOD + path_without_query`, signed
//! with RSA-PSS / SHA-256 / MGF1-SHA-256 / salt = digest length, base64 encoded.

use anyhow::{Context, Result};
use base64::Engine;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::SigningKey;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::RsaPrivateKey;
use sha2::Sha256;
use std::path::Path;

pub const H_KEY: &str = "KALSHI-ACCESS-KEY";
pub const H_SIG: &str = "KALSHI-ACCESS-SIGNATURE";
pub const H_TS: &str = "KALSHI-ACCESS-TIMESTAMP";

#[derive(Clone)]
pub struct KalshiAuth {
    key_id: String,
    signing_key: SigningKey<Sha256>,
}

impl std::fmt::Debug for KalshiAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KalshiAuth").field("key_id", &self.key_id).finish()
    }
}

impl KalshiAuth {
    pub fn from_pem(key_id: impl Into<String>, pem: &str) -> Result<Self> {
        let key = RsaPrivateKey::from_pkcs8_pem(pem)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
            .context("parsing Kalshi RSA private key (expected PKCS#8 or PKCS#1 PEM)")?;
        Ok(Self {
            key_id: key_id.into(),
            signing_key: SigningKey::<Sha256>::new(key),
        })
    }

    pub fn from_file(key_id: impl Into<String>, path: impl AsRef<Path>) -> Result<Self> {
        let pem = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("reading {}", path.as_ref().display()))?;
        Self::from_pem(key_id, &pem)
    }

    /// Build from `KALSHI_API_KEY_ID` + `KALSHI_PRIVATE_KEY_PATH`. `Ok(None)` if unset.
    pub fn from_env() -> Result<Option<Self>> {
        let key_id = std::env::var("KALSHI_API_KEY_ID").unwrap_or_default();
        let path = std::env::var("KALSHI_PRIVATE_KEY_PATH").unwrap_or_default();
        if key_id.is_empty() || path.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self::from_file(key_id, path)?))
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn sign(&self, ts_ms: i64, method: &str, path: &str) -> String {
        let path = path.split('?').next().unwrap_or(path);
        let msg = format!("{ts_ms}{method}{path}");
        let sig = self.signing_key.sign_with_rng(&mut rand::thread_rng(), msg.as_bytes());
        base64::engine::general_purpose::STANDARD.encode(sig.to_bytes())
    }

    /// The three auth headers for a request. `path` must be the full URL path
    /// from the host root (e.g. `/trade-api/v2/portfolio/balance`).
    pub fn headers(&self, method: &str, path: &str) -> [(&'static str, String); 3] {
        let ts = chrono::Utc::now().timestamp_millis();
        [
            (H_KEY, self.key_id.clone()),
            (H_SIG, self.sign(ts, method, path)),
            (H_TS, ts.to_string()),
        ]
    }
}
