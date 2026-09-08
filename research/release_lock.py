"""Economics settlement lock: scheduled releases (CPI, jobs, PCE, GDP at 12:30 UTC;
Fed decisions at 18:00 UTC) decide their Kalshi ladders instantly. How many
seconds does the tape take to reprice, and what edge was available in the
first 5 / 30 / 120 s after the print?

Method: for each settled economics market whose tape contains trades on a
weekday inside the release window, take the first release-window day with a
large move as the release; measure the price path from the release second.

Usage: python research/release_lock.py [--days 120]
Standard library only; ~1 request per market page, paced.
"""
import json
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

K = "https://external-api.kalshi.com/trade-api/v2"
WINDOWS = {  # series prefix -> (release hour UTC, minute)
    "KXCPI": (12, 30), "KXCPIYOY": (12, 30), "KXCPICOREYOY": (12, 30), "KXCOREPCE": (12, 30), "KXPCECORE": (12, 30), "KXPAYROLLS": (12, 30),
    "KXUNEMPLOYMENT": (12, 30), "KXU3": (12, 30), "KXGDP": (12, 30), "KXRETAIL": (12, 30), "KXPPI": (12, 30), "KXJOBS": (12, 30), "KXNFP": (12, 30),
    "KXFEDDECISION": (18, 0), "KXFED": (18, 0),
}


def get(url, tries=6, pause=0.4):
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                d = json.load(r)
            time.sleep(pause)
            return d
        except Exception as e:  # noqa: BLE001
            time.sleep(2 * (i + 1))
            last = e
    raise last


def trades(ticker):
    out, cur = [], ""
    while True:
        d = get(f"{K}/markets/trades?ticker={ticker}&limit=1000" + (f"&cursor={cur}" if cur else ""))
        out.extend(d.get("trades", []))
        cur = d.get("cursor", "")
        if not cur or not d.get("trades") or len(out) > 20000:
            break
    rows = []
    for t in out:
        ts = datetime.fromisoformat(t["created_time"].replace("Z", "+00:00"))
        rows.append((ts, float(t["yes_price_dollars"]), float(t["count_fp"])))
    rows.sort()
    return rows


def main():
    days = int(sys.argv[sys.argv.index("--days") + 1]) if "--days" in sys.argv else 120
    since = int(time.time()) - days * 86400
    series = get(f"{K}/series?category=Economics&limit=1000").get("series", [])
    series += get(f"{K}/series?category=Financials&limit=1000").get("series", [])
    cands = [s for s in series if any(s["ticker"].startswith(p) for p in WINDOWS)]
    print(f"{len(cands)} release-driven series")
    events = []
    for s in cands:
        hh, mm = next(v for p, v in WINDOWS.items() if s["ticker"].startswith(p))
        ms = get(f"{K}/markets?series_ticker={s['ticker']}&status=settled&min_close_ts={since}&limit=200&mve_filter=exclude").get("markets", [])
        ms = [m for m in ms if m.get("result") in ("yes", "no") and float(m.get("volume_fp") or 0) >= 300]
        for m in ms[:25]:
            try:
                tp = trades(m["ticker"])
            except Exception as e:  # noqa: BLE001
                print("  tape failed", m["ticker"], e)
                continue
            if len(tp) < 30:
                continue
            y = 1.0 if m["result"] == "yes" else 0.0
            # candidate release days: weekdays with trades inside [hh:mm, hh:mm+10min)
            by_day = defaultdict(list)
            for ts, px, q in tp:
                if ts.weekday() < 5 and (ts.hour, ts.minute) >= (hh, mm) and (ts.hour * 60 + ts.minute) < hh * 60 + mm + 10:
                    by_day[ts.date()].append((ts, px, q))
            best = None
            for d, win in by_day.items():
                pre = [px for ts, px, q in tp if ts < datetime(d.year, d.month, d.day, hh, mm, tzinfo=timezone.utc) and (datetime(d.year, d.month, d.day, hh, mm, tzinfo=timezone.utc) - ts).total_seconds() < 3600]
                if not pre:
                    continue
                pre_px = pre[-1]
                post = [px for ts, px, q in win]
                move = max(abs(p - pre_px) for p in post)
                if best is None or move > best[0]:
                    best = (move, d, pre_px, win)
            if not best or best[0] < 0.15:
                continue
            move, d, pre_px, win = best
            t0 = datetime(d.year, d.month, d.day, hh, mm, tzinfo=timezone.utc)
            after = [(ts, px, q) for ts, px, q in tp if ts >= t0 and (ts - t0).total_seconds() <= 3600]

            def px_at(secs):
                pts = [px for ts, px, q in after if (ts - t0).total_seconds() <= secs]
                return pts[-1] if pts else None

            def captured(secs):
                p = px_at(secs)
                return None if p is None else (p - pre_px) / (y - pre_px) if abs(y - pre_px) > 0.05 else None

            first = after[0] if after else None
            edge_first = None
            if first:
                fp = first[1]
                edge_first = (y - fp) if y == 1.0 else (fp - y)  # buying the eventual winner at the first post-release print
            events.append((m["ticker"], str(d), pre_px, y, (first[0] - t0).total_seconds() if first else None, px_at(5), px_at(30), px_at(120), captured(5), captured(30), captured(120), edge_first))
            print(f"  {m['ticker']:<34} {d} pre {pre_px:.2f} → settle {y:.0f} | first print +{(first[0]-t0).total_seconds():.0f}s at {first[1]:.2f} | 5s {px_at(5)} 30s {px_at(30)} 120s {px_at(120)} | buy winner at first print: {edge_first:+.2f}")
    if events:
        def med(vals):
            v = sorted(x for x in vals if x is not None)
            return v[len(v) // 2] if v else None
        print(f"\n{len(events)} release events")
        print(f"median seconds to first post-release print: {med([e[4] for e in events])}")
        print(f"median share of move captured by 5s: {med([e[8] for e in events])}, 30s: {med([e[9] for e in events])}, 120s: {med([e[10] for e in events])}")
        ef = [e[11] for e in events if e[11] is not None]
        print(f"buying the eventual winner at the FIRST post-release print: mean edge {sum(ef)/len(ef):+.3f}/contract over {len(ef)} events, win {sum(1 for x in ef if x>0)/len(ef):.0%}")
        with open("reports/release-lock.json", "w") as f:
            json.dump({"kind": "release-lock", "created_ms": int(time.time() * 1000), "events": [dict(zip(["ticker", "day", "pre_px", "settle", "first_print_secs", "px_5s", "px_30s", "px_120s", "cap_5s", "cap_30s", "cap_120s", "edge_first_print"], e)) for e in events]}, f, indent=1)


if __name__ == "__main__":
    main()
