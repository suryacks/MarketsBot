"""Minute-level settlement lock on daily-high markets, using our own recorded
order books (data/live-weather/books) and 1-minute ASOS observations (IEM).

For each recorded "≥ X°" market: find the first minute the running max ≥ X.
Then track the best ask afterwards: how many minutes until it reaches ≥ 0.97,
and the best (lowest) ask seen in the first 5 / 15 / 60 minutes. Same for
"between" buckets once the max passes above them (best bid → buy NO).

Usage: research/venv/bin/python research/weather_lock_intraday.py [YYYY-MM-DD]
"""
import glob
import io
import math
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq

STATIONS = {"KXHIGHNY": ("NYC", -4), "KXHIGHCHI": ("MDW", -5), "KXHIGHMIA": ("MIA", -4), "KXHIGHLAX": ("LAX", -7), "KXHIGHAUS": ("AUS", -5), "KXHIGHPHIL": ("PHL", -4), "KXHIGHDEN": ("DEN", -6)}


def fee(px):
    return math.ceil(0.07 * px * (1 - px) * 100) / 100


def iem_1min(station, day):
    """Routine + special METAR observations (current to the last hour; the 1-minute
    archive lags ~1.5 days). Specials fire on significant changes, so resolution is
    minutes when it matters."""
    q = urllib.parse.urlencode({
        "station": station, "data": "tmpf", "year1": day.year, "month1": day.month, "day1": day.day,
        "year2": (day + timedelta(days=2)).year, "month2": (day + timedelta(days=2)).month, "day2": (day + timedelta(days=2)).day,
        "tz": "Etc/UTC", "format": "onlycomma", "latlon": "no", "missing": "M", "trace": "T", "direct": "no",
    }) + "&report_type=2&report_type=3"
    url = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?" + q
    with urllib.request.urlopen(url, timeout=180) as r:
        text = r.read().decode()
    out = []
    for line in io.StringIO(text):
        p = line.strip().split(",")
        if len(p) < 3 or p[0] == "station":
            continue
        try:
            out.append((datetime.strptime(p[1][:16], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc), float(p[2])))
        except ValueError:
            continue
    out.sort()
    return out


def books_for(prefix):
    """ticker -> sorted list of (ts_ms, best_bid, best_ask) reconstructed from BookRow files."""
    rows = []
    for f in glob.glob("data/live-weather/books/**/*.parquet", recursive=True) + glob.glob("data/live-weather-paper/books/**/*.parquet", recursive=True):
        t = pq.read_table(f, filters=[("ticker", ">=", prefix), ("ticker", "<", prefix + "~")]) if False else pq.read_table(f)
        rows += [r for r in t.to_pylist() if r["ticker"].startswith(prefix)]
    rows.sort(key=lambda r: (r["ticker"], r["ts_ms"], r["seq"]))
    out = defaultdict(list)
    bids, asks = defaultdict(dict), defaultdict(dict)
    for r in rows:
        tk = r["ticker"]
        if r["kind"] == "snap":
            if r["snapshot_start"]:
                bids[tk].clear(); asks[tk].clear()
            (bids if r["is_bid"] else asks)[tk][r["px"]] = r["qty"]
        elif r["kind"] == "delta":
            d = (bids if r["is_bid"] else asks)[tk]
            d[r["px"]] = d.get(r["px"], 0) + r["qty"]
            if d[r["px"]] <= 0:
                del d[r["px"]]
        else:
            d = (bids if r["is_bid"] else asks)[tk]
            if r["qty"] <= 0:
                d.pop(r["px"], None)
            else:
                d[r["px"]] = r["qty"]
        bb = max(bids[tk]) / 10000 if bids[tk] else None
        ba = min(asks[tk]) / 10000 if asks[tk] else None
        out[tk].append((r["ts_ms"], bb, ba))
    return out


def main():
    day = datetime.strptime(sys.argv[1], "%Y-%m-%d").replace(tzinfo=timezone.utc) if len(sys.argv) > 1 and not sys.argv[1].startswith("-") else datetime.now(timezone.utc) - timedelta(days=0)
    tag = day.strftime("%y%b%d").upper()  # 26SEP08
    print(f"market day {day.date()} (ticker tag {tag})")
    results = []
    for series, (stn, off) in STATIONS.items():
        prefix = f"{series}-{tag}-"
        books = books_for(prefix)
        if not books:
            print(f"  {series}: no recorded books"); continue
        try:
            obs = iem_1min(stn, day - timedelta(days=1))
        except Exception as e:  # noqa: BLE001
            print(f"  {series}: obs failed {e}"); time.sleep(10); continue
        time.sleep(8)
        day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc) - timedelta(hours=off)
        day_end = day_start + timedelta(hours=24)
        run = []  # (ts, running max)
        mx = -999
        for t, v in obs:
            if day_start <= t <= day_end:
                mx = max(mx, v); run.append((t, mx))
        if not run:
            print(f"  {series}: no obs in window"); continue
        print(f"  {series}: {len(books)} markets with books, {len(run)} obs minutes, day max so far {mx:.0f}F")
        for tk, path in books.items():
            t_ = tk.rsplit("-", 1)[-1]
            if t_.startswith("B"):
                c = float(t_[1:]); lo, hi = math.floor(c), math.ceil(c); kind = "between"
            elif t_.startswith("T"):
                lo = int(math.floor(float(t_[1:]))) + 1; hi = 999; kind = "greater"  # T80 == "greater than 80" => >= 81 for whole degrees
            else:
                continue
            cross = next((t for t, m in run if (m >= lo if kind == "greater" else m > hi)), None)
            if cross is None:
                continue
            cross_ms = int(cross.timestamp() * 1000)
            after = [(ts, bb, ba) for ts, bb, ba in path if ts >= cross_ms]
            if not after:
                continue
            # time until the book agrees the outcome is locked
            def locked(bb, ba):
                return (ba is not None and ba >= 0.97) if kind == "greater" else (bb is not None and bb <= 0.03)
            first_locked = next((ts for ts, bb, ba in after if locked(bb, ba)), None)
            lag_min = (first_locked - cross_ms) / 60000 if first_locked else None
            def best_edge(window_min):
                w = [x for x in after if x[0] <= cross_ms + window_min * 60000]
                if kind == "greater":
                    asks = [ba for _, _, ba in w if ba is not None]
                    return (1 - min(asks) - fee(min(asks))) if asks else None
                bids = [bb for _, bb, _ in w if bb is not None]
                return (max(bids) - fee(max(bids))) if bids else None
            e5, e15, e60 = best_edge(5), best_edge(15), best_edge(60)
            pre = [x for x in path if x[0] < cross_ms][-1:]
            results.append((tk, kind, cross.strftime("%H:%M"), lag_min, e5, e15, e60, pre[0][2] if pre and kind == "greater" else (pre[0][1] if pre else None)))
    print(f"\n{'ticker':<30} {'kind':<8} {'crossed':>7} {'lag→locked':>10} {'edge≤5m':>8} {'edge≤15m':>9} {'edge≤60m':>9} {'px before':>9}")
    for r in sorted(results, key=lambda r: -(r[4] or -1)):
        print(f"{r[0]:<30} {r[1]:<8} {r[2]:>7} {('%.0f min' % r[3]) if r[3] is not None else 'never':>10} {('%+.3f' % r[4]) if r[4] is not None else '–':>8} {('%+.3f' % r[5]) if r[5] is not None else '–':>9} {('%+.3f' % r[6]) if r[6] is not None else '–':>9} {('%.2f' % r[7]) if r[7] is not None else '–':>9}")
    if results:
        e5 = [r[4] for r in results if r[4] is not None]
        print(f"\n{len(results)} lock events; edge ≥ 2¢ within 5 min in {sum(1 for e in e5 if e >= 0.02)} of {len(e5)}; median lag to locked {sorted([r[3] for r in results if r[3] is not None])[len([r for r in results if r[3] is not None])//2] if any(r[3] is not None for r in results) else 'n/a'} min")


if __name__ == "__main__":
    main()
