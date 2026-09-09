"""Authenticated Kalshi REST for research scripts.

The anonymous API answers, but it returns null for `yes_bid`, `yes_ask`, `volume`
and `open_interest` on every market -- including ones we have traded ourselves.
Any research that filtered on those fields was therefore reading absence of data
as absence of liquidity. Use this instead of urlopen for anything price-related.

Signing is RSA-PSS/SHA-256 over "<timestamp_ms><METHOD><path>", per Kalshi's spec.
"""
import base64, json, os, time, urllib.parse, urllib.request
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

BASE = "https://external-api.kalshi.com/trade-api/v2"


def _load_key():
    path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "kalshi-private-key.pem")
    with open(path, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)


_KEY = None
_KID = None


def _creds():
    global _KEY, _KID
    if _KEY is None:
        # tolerate being run without the shell env loaded
        if not os.environ.get("KALSHI_API_KEY_ID") and os.path.exists(".env"):
            for line in open(".env"):
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())
        _KEY = _load_key()
        _KID = os.environ["KALSHI_API_KEY_ID"]
    return _KEY, _KID


def get(path, params=None, tries=5):
    """GET a Kalshi path (e.g. "/markets"); returns {} on repeated failure."""
    key, kid = _creds()
    qs = ("?" + urllib.parse.urlencode(params)) if params else ""
    for i in range(tries):
        try:
            ts = str(int(time.time() * 1000))
            sig = key.sign(
                (ts + "GET" + "/trade-api/v2" + path).encode(),
                padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                hashes.SHA256(),
            )
            req = urllib.request.Request(BASE + path + qs, headers={
                "KALSHI-ACCESS-KEY": kid,
                "KALSHI-ACCESS-TIMESTAMP": ts,
                "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
                "User-Agent": "marketsbot-research",
            })
            with urllib.request.urlopen(req, timeout=40) as r:
                d = json.load(r)
            time.sleep(0.12)
            return d
        except Exception:  # noqa: BLE001
            time.sleep(1.5 * (i + 1))
    return {}


def markets(**params):
    """Page through /markets, yielding every market."""
    params.setdefault("limit", 1000)
    cursor = ""
    while True:
        p = dict(params)
        if cursor:
            p["cursor"] = cursor
        d = get("/markets", p)
        ms = d.get("markets", [])
        if not ms:
            return
        yield from ms
        cursor = d.get("cursor") or ""
        if not cursor:
            return
