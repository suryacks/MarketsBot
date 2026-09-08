"""Minute-level lead–lag between Polymarket and Kalshi on the same event:
the September 2026 FOMC decision ("25 bp cut" / "no change").

Kalshi: KXFEDDECISION-26SEP markets (second-level trade tape → 1-min last price).
Polymarket: the event's markets via Gamma (prices-history at 1-min fidelity).
Reports correlation of 1-min changes at lags −5..+5 minutes and the size of
price gaps; a peak at a positive lag (Polymarket leads) is the tradeable window.
"""
import json
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

K = "https://external-api.kalshi.com/trade-api/v2"
G = "https://gamma-api.polymarket.com"
C = "https://clob.polymarket.com"


def get(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "marketsbot-research"}), timeout=30) as r:
        d = json.load(r)
    time.sleep(0.35)
    return d


def kalshi_minutes(ticker, days=14):
    out, cur = [], ""
    since = int(time.time()) - days * 86400
    while True:
        d = get(f"{K}/markets/trades?ticker={ticker}&limit=1000&min_ts={since}" + (f"&cursor={cur}" if cur else ""))
        out.extend(d.get("trades", []))
        cur = d.get("cursor", "")
        if not cur or not d.get("trades") or len(out) > 40000:
            break
    m = {}
    for t in sorted(out, key=lambda t: t["created_time"]):
        ts = int(datetime.fromisoformat(t["created_time"].replace("Z", "+00:00")).timestamp()) // 60 * 60
        m[ts] = float(t["yes_price_dollars"])
    return m


def poly_minutes(token, days=14):
    end = int(time.time()); start = end - days * 86400
    d = get(f"{C}/prices-history?market={token}&startTs={start}&endTs={end}&fidelity=1")
    return {int(p["t"]) // 60 * 60: float(p["p"]) for p in d.get("history", [])}


def corr(x, y):
    n = len(x)
    if n < 20:
        return None
    mx, my = sum(x) / n, sum(y) / n
    sx = sum((a - mx) ** 2 for a in x) ** 0.5; sy = sum((b - my) ** 2 for b in y) ** 0.5
    return sum((a - mx) * (b - my) for a, b in zip(x, y)) / (sx * sy) if sx and sy else 0.0


def main():
    km = get(f"{K}/markets?series_ticker=KXFEDDECISION&status=open&limit=50&mve_filter=exclude").get("markets", [])
    print("Kalshi Fed markets:", [(m["ticker"], m.get("yes_bid_dollars"), m.get("yes_ask_dollars")) for m in km])
    evs = get(f"{G}/events?limit=100&active=true&closed=false&order=volume24hr&ascending=false")
    fed = [e for e in evs if "fed" in e.get("title", "").lower() and ("september" in e.get("title", "").lower() or "sept" in e.get("title", "").lower() or "rate" in e.get("title", "").lower())]
    for e in fed[:3]:
        print("Polymarket event:", e["title"], [(m.get("question")[:40], m.get("outcomePrices")) for m in e.get("markets", [])[:6]])
    pairs = []
    for m in km:
        lbl = m["ticker"].rsplit("-", 1)[-1]  # H25 / H0 / H50
        for e in fed[:3]:
            for pm in e.get("markets", []):
                q = pm.get("question", "").lower()
                if (lbl == "H25" and "25" in q and "decrease" in q) or (lbl == "H0" and ("no change" in q or "unchanged" in q)) or (lbl == "H50" and "50" in q and "decrease" in q):
                    tok = json.loads(pm.get("clobTokenIds", "[]") or "[]")
                    if tok:
                        pairs.append((m["ticker"], pm["question"][:50], tok[0]))
    print(f"{len(pairs)} matched pairs")
    for kt, pq_, tok in pairs:
        kmin, pmin = kalshi_minutes(kt), poly_minutes(tok)
        common = sorted(set(kmin) & set(pmin))
        print(f"\n{kt} ~ {pq_}: kalshi minutes {len(kmin)}, poly minutes {len(pmin)}, common {len(common)}")
        if len(common) < 60:
            continue
        kx = {t: kmin[t] for t in common}; px = {t: pmin[t] for t in common}
        gaps = [kx[t] - px[t] for t in common]
        print(f"  mean gap kalshi−poly {sum(gaps)/len(gaps):+.3f}, max |gap| {max(abs(g) for g in gaps):.3f}")
        ks = [(t, kx[t] - kx[p]) for p, t in zip(common, common[1:]) if t - p == 60]
        ps = {t: px[t] - px[p] for p, t in zip(common, common[1:]) if t - p == 60}
        for lag in range(-5, 6):
            xs, ys = [], []
            for t, dk in ks:
                tp = t - lag * 60
                if tp in ps:
                    xs.append(dk); ys.append(ps[tp])
            c = corr(xs, ys)
            print(f"  corr(Δkalshi_t, Δpoly_t{'+' if -lag>=0 else ''}{-lag}) = {c:+.3f} (n={len(xs)})" if c is not None else f"  lag {lag}: n too small")
        print("  (positive correlation at 'Δpoly_t-k' for k>0 means Polymarket moved first)")


if __name__ == "__main__":
    main()
