# NOTE: Kalshi names these `yes_bid_dollars`/`yes_ask_dollars` (decimal strings), not
# `yes_bid`/`yes_ask`. Reading the short names silently yields None on every market and
# makes a liquid book look empty -- which is exactly how a scan once "proved" that
# 27,000 combo markets had no liquidity at all.
"""Cross-venue lead–lag and price gaps: Kalshi vs Polymarket on the same events.

1. Pull top-volume active markets on both venues, match them by title tokens
   (Jaccard similarity on normalized words + numbers), keep confident pairs.
2. For each pair pull hourly price histories (Kalshi candlesticks, Polymarket
   prices-history at 60-minute fidelity) over the last N days.
3. Report: mean/max absolute price gap, and the lead–lag cross-correlation of
   hourly price changes (does Polymarket's move at hour t predict Kalshi's move
   at t+1, or vice versa?).  A positive Polymarket→Kalshi lag means a "follow
   Polymarket" strategy on Kalshi is worth backtesting.

Usage: python research/cross_venue.py [--days 14] [--top 400] [--min-sim 0.5]
Standard library only.
"""
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

K = "https://external-api.kalshi.com/trade-api/v2"
G = "https://gamma-api.polymarket.com"
C = "https://clob.polymarket.com"
STOP = set("the a an of to in on at by for will be is are and or vs v than more less over under before after with from into up down this that".split())


def get(url, tries=5, pause=0.35):
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "marketsbot-research"}), timeout=30) as r:
                d = json.load(r)
            time.sleep(pause)
            return d
        except Exception as e:  # noqa: BLE001
            time.sleep(1.5 * (i + 1))
            last = e
    raise last


def arg(name, default):
    return sys.argv[sys.argv.index(name) + 1] if name in sys.argv else default


MONTHS = {m: i for i, m in enumerate("jan feb mar apr may jun jul aug sep oct nov dec".split(), 1)}
SYN = {"yes": "", "no": "", "market": "", "price": "", "above": ">", "below": "<", "greater": ">", "less": "<", "higher": ">", "lower": "<",
       "percent": "%", "pct": "%", "points": "pts", "point": "pts", "bitcoin": "btc", "ethereum": "eth", "solana": "sol", "federal": "fed",
       "reserve": "", "rate": "rates", "cut": "cuts", "hike": "hikes", "meeting": "", "september": "sep", "october": "oct", "november": "nov",
       "december": "dec", "january": "jan", "february": "feb", "march": "mar", "april": "apr", "june": "jun", "july": "jul", "august": "aug"}


def tokens(s):
    s = s.lower().replace("’", "'").replace("$", " ")
    words = re.findall(r"[a-z0-9\.\-'%]+", s)
    out = set()
    for w in words:
        w = w.strip(".-'")
        w = SYN.get(w, w)
        if not w or w in STOP or len(w) < 2:
            continue
        # numbers: normalize 79,000 / 79k / 79000
        if re.fullmatch(r"[0-9\.,]+k?", w):
            w = w.replace(",", "")
            if w.endswith("k"):
                w = str(int(float(w[:-1]) * 1000))
        out.add(w)
    return out


def jaccard(a, b):
    if not a or not b:
        return 0.0
    inter = a & b
    # informative overlap: at least two shared tokens and one of them a proper-ish token (number or 4+ letters)
    if len(inter) < 2 or not any(t.isdigit() or len(t) >= 4 for t in inter):
        return 0.0
    return len(inter) / len(a | b)


def kalshi_markets(top):
    """Open markets from the most active series in the categories both venues share."""
    out = []
    for cat in ("Politics", "Economics", "World", "Elections", "Sports", "Crypto", "Companies", "Science and Technology"):
        series = get(f"{K}/series?category={urllib.parse.quote(cat)}&limit=1000").get("series", [])
        # cheap activity proxy: try each series' open markets, keep the ones with volume
        for s in series[:400]:
            try:
                ms = get(f"{K}/markets?series_ticker={s['ticker']}&status=open&limit=50&mve_filter=exclude", pause=0.3).get("markets", [])
            except Exception:  # noqa: BLE001
                continue
            for m in ms:
                if float(m.get("volume_fp") or 0) >= 500:
                    m["_cat"] = cat
                    m["_series_title"] = s.get("title", "")
                    out.append(m)
            if len(out) >= top * 3:
                break
        if len(out) >= top * 3:
            break
    out.sort(key=lambda m: -float(m.get("volume_fp") or 0))
    return out[:top]


def poly_markets(top):
    out = []
    off = 0
    while len(out) < top:
        evs = get(f"{G}/events?limit=100&offset={off}&active=true&closed=false&order=volume24hr&ascending=false")
        if not evs:
            break
        for e in evs:
            for m in e.get("markets", []):
                m["_event_title"] = e.get("title", "")
                out.append(m)
        off += 100
        if off > 3000:
            break
    return out[:top]


def kalshi_hist(series, ticker, start, end):
    d = get(f"{K}/series/{series}/markets/{ticker}/candlesticks?start_ts={start}&end_ts={end}&period_interval=60")
    pts = []
    for c in d.get("candlesticks", []):
        b, a = c.get("yes_bid_dollars", {}).get("close_dollars"), c.get("yes_ask_dollars", {}).get("close_dollars")
        if b and a and 0 < float(b) and float(a) < 1:
            pts.append((c["end_period_ts"] // 3600 * 3600, (float(a) + float(b)) / 2))
        elif c.get("price", {}).get("close_dollars"):
            pts.append((c["end_period_ts"] // 3600 * 3600, float(c["price"]["close_dollars"])))
    return dict(pts)


def poly_hist(token, start, end):
    d = get(f"{C}/prices-history?market={token}&startTs={start}&endTs={end}&fidelity=60")
    return {p["t"] // 3600 * 3600: float(p["p"]) for p in d.get("history", [])}


def xcorr(kx, px):
    """Lead–lag on hourly changes. Returns (corr_same_hour, corr_poly_leads, corr_kalshi_leads, n)."""
    hours = sorted(set(kx) & set(px))
    ks, ps = [], []
    for h0, h1 in zip(hours, hours[1:]):
        if h1 - h0 != 3600:
            continue
        ks.append(kx[h1] - kx[h0])
        ps.append(px[h1] - px[h0])
    n = len(ks)
    if n < 12:
        return None

    def corr(x, y):
        mx, my = sum(x) / len(x), sum(y) / len(y)
        sx = sum((a - mx) ** 2 for a in x) ** 0.5
        sy = sum((b - my) ** 2 for b in y) ** 0.5
        return sum((a - mx) * (b - my) for a, b in zip(x, y)) / (sx * sy) if sx > 0 and sy > 0 else 0.0

    return corr(ks, ps), corr(ks[1:], ps[:-1]), corr(ks[:-1], ps[1:]), n


def main():
    days = int(arg("--days", 14))
    top = int(arg("--top", 400))
    min_sim = float(arg("--min-sim", 0.35))
    km = kalshi_markets(top)
    pm = poly_markets(top)
    print(f"{len(km)} Kalshi markets, {len(pm)} Polymarket markets")
    ptok = [(tokens(m.get("question", "") + " " + m.get("_event_title", "")), m) for m in pm]
    pairs = []
    for m in km:
        kt = tokens(m.get("title", "") + " " + m.get("yes_sub_title", "") + " " + m.get("_series_title", ""))
        best = max(((jaccard(kt, t), p) for t, p in ptok), key=lambda x: x[0], default=(0, None))
        if best[0] >= min_sim and best[1]:
            pairs.append((best[0], m, best[1]))
    pairs.sort(key=lambda x: -x[0])
    print(f"{len(pairs)} confident pairs (sim ≥ {min_sim})\n")
    end = int(time.time())
    start = end - days * 86400
    rows = []
    for sim, m, p in pairs[:60]:
        tok = json.loads(p.get("clobTokenIds", "[]") or "[]")
        if not tok:
            continue
        series = m["event_ticker"].split("-")[0]
        try:
            kh = kalshi_hist(series, m["ticker"], start, end)
            ph = poly_hist(tok[0], start, end)
        except Exception as e:  # noqa: BLE001
            print("  fetch failed", m["ticker"], e)
            continue
        common = sorted(set(kh) & set(ph))
        if len(common) < 12:
            continue
        gaps = [kh[h] - ph[h] for h in common]
        mean_gap = sum(gaps) / len(gaps)
        max_gap = max(abs(g) for g in gaps)
        xc = xcorr(kh, ph)
        rows.append((sim, m["ticker"], p.get("question", "")[:50], len(common), mean_gap, max_gap, xc))
        c0, cp, ck, n = xc if xc else (float("nan"),) * 4
        print(f"{sim:.2f} {m['ticker']:<34} ~ {p.get('question','')[:44]:<44} hrs={len(common):3d} gap mean {mean_gap:+.3f} max {max_gap:.3f} | corr same {c0:+.2f} poly→kalshi {cp:+.2f} kalshi→poly {ck:+.2f}")
    if rows:
        valid = [r for r in rows if r[6]]
        if valid:
            avg = lambda i: sum(r[6][i] for r in valid) / len(valid)
            print(f"\n{len(valid)} pairs with ≥12 common hours: mean corr same-hour {avg(0):+.3f}, Polymarket→Kalshi next hour {avg(1):+.3f}, Kalshi→Polymarket next hour {avg(2):+.3f}")
            print("A Polymarket→Kalshi value clearly above Kalshi→Polymarket means Kalshi lags; that lag is the tradeable window.")
        print(f"mean |gap| across pairs: {sum(abs(r[4]) for r in rows)/len(rows):.3f}")
    out = {"kind": "cross-venue", "created_ms": int(time.time() * 1000), "days": days,
           "pairs": [{"sim": r[0], "kalshi": r[1], "polymarket": r[2], "hours": r[3], "mean_gap": r[4], "max_gap": r[5],
                      "corr_same": r[6][0] if r[6] else None, "corr_poly_leads": r[6][1] if r[6] else None, "corr_kalshi_leads": r[6][2] if r[6] else None} for r in rows]}
    with open("reports/cross-venue.json", "w") as f:
        json.dump(out, f, indent=1)


if __name__ == "__main__":
    main()
