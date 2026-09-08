"""How fast do Kalshi macro markets reprice after a scheduled data release?

For settled markets in economics series whose title matches the given keywords
(CPI, jobs, unemployment, Fed…), pull the public trade tape, locate the release
moment as the largest 1-minute price move, and measure how much of the
pre-release → settlement distance the market had covered 5 s / 30 s / 2 min /
10 min after the first post-release trade. If the price 30 s after a release is
still far from where it settles, a bot that reads the release at t+0 has a window.

Usage: python research/release_speed.py [--days 180] [--keywords CPI,jobs,unemployment,payroll,fed]
Standard library only; Kalshi public endpoints paced at ~3 req/s.
"""
import json
import statistics
import sys
import time
import urllib.request
from datetime import datetime, timezone

K = "https://external-api.kalshi.com/trade-api/v2"


def get(url, tries=6):
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                d = json.load(r)
            time.sleep(0.35)
            return d
        except Exception as e:  # noqa: BLE001
            time.sleep(1.5 * (i + 1))
            last = e
    raise last


def arg(name, default):
    if name in sys.argv:
        return sys.argv[sys.argv.index(name) + 1]
    return default


def all_trades(ticker):
    out, cur = [], ""
    while True:
        d = get(f"{K}/markets/trades?ticker={ticker}&limit=1000" + (f"&cursor={cur}" if cur else ""))
        out.extend(d.get("trades", []))
        cur = d.get("cursor", "")
        if not cur or not d.get("trades"):
            break
    for t in out:
        t["_ts"] = datetime.fromisoformat(t["created_time"].replace("Z", "+00:00")).timestamp()
        t["_px"] = float(t["yes_price_dollars"])
        t["_q"] = float(t["count_fp"])
    out.sort(key=lambda t: t["_ts"])
    return out


def vwap(trades):
    q = sum(t["_q"] for t in trades)
    return sum(t["_px"] * t["_q"] for t in trades) / q if q else None


def main():
    days = int(arg("--days", 180))
    kws = [k.strip().lower() for k in arg("--keywords", "cpi,jobs,unemployment,payroll,fed,inflation,gdp").split(",")]
    since = int(time.time()) - days * 86400
    series = [s for s in get(f"{K}/series?category=Economics&limit=1000").get("series", [])
              if any(k in (s.get("title", "") + " " + s["ticker"]).lower() for k in kws)]
    print(f"{len(series)} economics series match {kws}")
    rows = []
    for s in series:
        ms = get(f"{K}/markets?series_ticker={s['ticker']}&status=settled&min_close_ts={since}&limit=200&mve_filter=exclude").get("markets", [])
        ms = [m for m in ms if m.get("result") in ("yes", "no") and float(m.get("volume_fp", "0") or 0) >= 2000]
        for m in ms[:12]:
            tr = all_trades(m["ticker"])
            if len(tr) < 200:
                continue
            # largest 60-second VWAP move
            best = None
            t0 = tr[0]["_ts"]
            i = 0
            while i < len(tr):
                w = [t for t in tr if tr[i]["_ts"] - 60 <= t["_ts"] < tr[i]["_ts"]]
                a = [t for t in tr if tr[i]["_ts"] <= t["_ts"] < tr[i]["_ts"] + 60]
                if len(w) >= 3 and len(a) >= 3:
                    mv = abs(vwap(a) - vwap(w))
                    if best is None or mv > best[0]:
                        best = (mv, tr[i]["_ts"])
                i += max(1, len(tr) // 400)
            if not best or best[0] < 0.10:
                continue
            mv, t_rel = best
            y = 1.0 if m["result"] == "yes" else 0.0
            pre = vwap([t for t in tr if t_rel - 600 <= t["_ts"] < t_rel - 60])
            if pre is None:
                continue
            after = {}
            for lab, sec in (("5s", 5), ("30s", 30), ("2m", 120), ("10m", 600), ("1h", 3600)):
                w = [t for t in tr if t_rel <= t["_ts"] < t_rel + sec]
                after[lab] = vwap(w[-max(1, len(w) // 4):]) if w else None  # last quarter of the window
            dist = y - pre
            cov = {lab: (None if v is None or abs(dist) < 0.05 else (v - pre) / dist) for lab, v in after.items()}
            rows.append((s["ticker"], m["ticker"], datetime.fromtimestamp(t_rel, timezone.utc).strftime("%m-%d %H:%M:%S"), pre, after, y, cov, len(tr)))
            print(f"{m['ticker']:<36} jump@{rows[-1][2]} pre {pre:.2f} → 5s {after['5s'] and round(after['5s'],2)} 30s {after['30s'] and round(after['30s'],2)} 2m {after['2m'] and round(after['2m'],2)} 10m {after['10m'] and round(after['10m'],2)} settle {y:.0f}  captured: " + " ".join(f"{k}={'' if v is None else f'{100*v:.0f}%'}" for k, v in cov.items()))
    print(f"\n{len(rows)} release events")
    for lab in ("5s", "30s", "2m", "10m", "1h"):
        vals = [r[6][lab] for r in rows if r[6][lab] is not None]
        if vals:
            print(f"  median share of pre→settle move captured by {lab:>3}: {100*statistics.median(vals):5.0f}%   (mean {100*statistics.mean(vals):5.0f}%, n={len(vals)})")


if __name__ == "__main__":
    main()
