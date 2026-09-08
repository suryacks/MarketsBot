"""Economics settlement lock with the OFFICIAL release calendars.

Scheduled prints decide their Kalshi ladders instantly. For every settled
market whose life spans one of its release timestamps, measure how the tape
repriced after the exact second of the print: seconds to first trade, the
price path at +5/+30/+120 s, and the edge available buying the eventual winner
at the first post-release print.

Calendars are scraped from the agencies (no key):
  BLS   https://www.bls.gov/schedule/news_release/{cpi,empsit,ppi}.htm   (08:30 ET)
  BEA   https://www.bea.gov/news/schedule                                   (08:30 ET)
  FOMC  https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm    (14:00 ET)

Usage: python research/release_lock.py [--days 150]
"""
import json
import re
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone

K = "https://external-api.kalshi.com/trade-api/v2"
MONTHS = {m: i for i, m in enumerate(["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"], 1)}
MON3 = {m[:3]: i for m, i in MONTHS.items()}


def get(url, tries=6, pause=0.4, headers=None):
    req = urllib.request.Request(url, headers=headers or {"User-Agent": "Mozilla/5.0 (research script)"})
    for i in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                d = r.read().decode(errors="ignore")
            time.sleep(pause)
            return d
        except Exception as e:  # noqa: BLE001
            time.sleep(2 * (i + 1))
            last = e
    raise last


def et_to_utc(y, m, d, hh, mm):
    """US Eastern → UTC (DST: second Sunday of March .. first Sunday of November)."""
    dt = datetime(y, m, d, hh, mm)
    mar = datetime(y, 3, 8) + timedelta(days=(6 - datetime(y, 3, 8).weekday()) % 7)
    nov = datetime(y, 11, 1) + timedelta(days=(6 - datetime(y, 11, 1).weekday()) % 7)
    off = 4 if mar <= dt < nov else 5
    return (dt + timedelta(hours=off)).replace(tzinfo=timezone.utc)


def bls_dates(page):
    html = get(f"https://www.bls.gov/schedule/news_release/{page}.htm")
    out = set()
    for m in re.finditer(r"([A-Z][a-z]+)\.?\s+(\d{1,2}),\s+(\d{4})", html):
        mon = MONTHS.get(m.group(1).lower()) or MON3.get(m.group(1).lower()[:3])
        if mon:
            out.add(et_to_utc(int(m.group(3)), mon, int(m.group(2)), 8, 30))
    return out


def bea_dates():
    html = get("https://www.bea.gov/news/schedule")
    out = {"pce": set(), "gdp": set()}
    # rows like: "July 31, 2026 ... Personal Income and Outlays, June 2026" / "Gross Domestic Product ..."
    for block in re.split(r"<tr", html):
        m = re.search(r"([A-Z][a-z]+)\s+(\d{1,2}),\s+(\d{4})", block)
        if not m:
            continue
        mon = MONTHS.get(m.group(1).lower())
        if not mon:
            continue
        ts = et_to_utc(int(m.group(3)), mon, int(m.group(2)), 8, 30)
        low = block.lower()
        if "personal income" in low:
            out["pce"].add(ts)
        if "gross domestic product" in low or "gdp" in low:
            out["gdp"].add(ts)
    return out


def fomc_dates():
    html = get("https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm")
    out = set()
    # "January 27-28" style within a year section
    for ysec in re.split(r"(?=<h4[^>]*>\s*(?:19|20)\d\d)", html):
        ym = re.search(r"((?:19|20)\d\d)", ysec)
        if not ym:
            continue
        y = int(ym.group(1))
        for m in re.finditer(r"<strong>([A-Z][a-z]+)(?:/[A-Z][a-z]+)?</strong>\s*(?:<[^>]+>\s*)*(\d{1,2})(?:-(\d{1,2}))?", ysec):
            mon = MONTHS.get(m.group(1).lower())
            if not mon:
                continue
            day = int(m.group(3) or m.group(2))  # second day of a two-day meeting
            out.add(et_to_utc(y, mon, day, 14, 0))
    return out


def trades(ticker):
    out, cur = [], ""
    while True:
        d = json.loads(get(f"{K}/markets/trades?ticker={ticker}&limit=1000" + (f"&cursor={cur}" if cur else "")))
        out.extend(d.get("trades", []))
        cur = d.get("cursor", "")
        if not cur or not d.get("trades") or len(out) > 30000:
            break
    rows = [(datetime.fromisoformat(t["created_time"].replace("Z", "+00:00")), float(t["yes_price_dollars"]), float(t["count_fp"])) for t in out]
    rows.sort()
    return rows


SERIES = {  # prefix -> calendar key
    "KXCPI": "cpi", "KXCPIYOY": "cpi", "KXCPICORE": "cpi", "KXCPICOREYOY": "cpi", "KXECONSTATCPI": "cpi",
    "KXPAYROLLS": "empsit", "KXUNEMPLOYMENT": "empsit", "KXU3": "empsit", "KXNFP": "empsit", "KXJOBS": "empsit",
    "KXPPI": "ppi", "KXPCECORE": "pce", "KXCOREPCE": "pce", "KXPCE": "pce", "KXGDP": "gdp",
    "KXFEDDECISION": "fomc", "KXFED": "fomc", "KXLARGECUT": "fomc",
}


def main():
    days = int(sys.argv[sys.argv.index("--days") + 1]) if "--days" in sys.argv else 150
    since = int(time.time()) - days * 86400
    # The agencies block scripted fetches (403); dates were fetched once and stored.
    with open("research/release_calendar.json") as f:
        raw = json.load(f)
    cal = {}
    for k, v in raw.items():
        if k.startswith("_"):
            continue
        s = set()
        for d in v:
            dt = datetime.strptime(d, "%Y-%m-%d %H:%M")
            s.add(et_to_utc(dt.year, dt.month, dt.day, dt.hour, dt.minute))
        cal[k] = s
    for k, v in cal.items():
        recent = sorted(t for t in v if t.timestamp() >= since)
        print(f"calendar {k}: {len(v)} dates, recent: {[t.strftime('%m-%d %H:%MZ') for t in recent[:6]]}")
    series = json.loads(get(f"{K}/series?category=Economics&limit=1000")).get("series", []) + json.loads(get(f"{K}/series?category=Financials&limit=1000")).get("series", [])
    events = []
    for s in series:
        key = next((v for p, v in SERIES.items() if s["ticker"] == p or s["ticker"].startswith(p)), None)
        if not key or not cal.get(key):
            continue
        ms = json.loads(get(f"{K}/markets?series_ticker={s['ticker']}&status=settled&min_close_ts={since}&limit=200&mve_filter=exclude")).get("markets", [])
        ms = [m for m in ms if m.get("result") in ("yes", "no") and float(m.get("volume_fp") or 0) >= 200]
        for m in ms[:30]:
            open_t = datetime.fromisoformat(m["open_time"].replace("Z", "+00:00"))
            close_t = datetime.fromisoformat(m["close_time"].replace("Z", "+00:00"))
            rel = [t for t in cal[key] if open_t < t < close_t + timedelta(hours=1)]
            if not rel:
                continue
            t0 = max(rel)  # the last scheduled print before close decides it
            try:
                tp = trades(m["ticker"])
            except Exception as e:  # noqa: BLE001
                print("  tape failed", m["ticker"], e)
                continue
            pre = [px for ts, px, q in tp if ts < t0 and (t0 - ts).total_seconds() < 6 * 3600]
            after = [(ts, px, q) for ts, px, q in tp if ts >= t0 and (ts - t0).total_seconds() <= 3600]
            if not pre or not after:
                continue
            pre_px = pre[-1]
            y = 1.0 if m["result"] == "yes" else 0.0

            def px_at(secs):
                pts = [px for ts, px, q in after if (ts - t0).total_seconds() <= secs]
                return pts[-1] if pts else None

            first = after[0]
            edge_first = (y - first[1]) if y == 1.0 else (first[1] - y)
            # prints on the losing side within the first 5 minutes: size-weighted edge available to a fast trader
            wrong = [(px, q) for ts, px, q in after if (ts - t0).total_seconds() <= 300 and ((y == 1.0 and px < 0.9) or (y == 0.0 and px > 0.1))]
            wrong_qty = sum(q for _, q in wrong)
            wrong_edge = (sum(((1 - px) if y == 1.0 else px) * q for px, q in wrong) / wrong_qty) if wrong_qty else 0.0
            events.append({"ticker": m["ticker"], "release": t0.isoformat(), "pre_px": pre_px, "settle": y, "first_print_secs": (first[0] - t0).total_seconds(), "first_px": first[1],
                           "px_5s": px_at(5), "px_30s": px_at(30), "px_120s": px_at(120), "edge_first_print": edge_first, "wrong_side_qty_5min": wrong_qty, "wrong_side_edge": wrong_edge})
            print(f"  {m['ticker']:<32} {t0.strftime('%m-%d %H:%M')}Z pre {pre_px:.2f}→{y:.0f} | 1st print +{(first[0]-t0).total_seconds():5.0f}s @{first[1]:.2f} | +30s {px_at(30)} +120s {px_at(120)} | losing-side prints in 5 min: {wrong_qty:6.0f} contracts, avg edge {wrong_edge:.2f}")
    if events:
        def med(v):
            v = sorted(x for x in v if x is not None)
            return v[len(v) // 2] if v else None
        print(f"\n{len(events)} release events aligned to official calendars")
        print(f"median seconds to first print after release: {med([e['first_print_secs'] for e in events]):.0f}")
        print(f"median edge buying the winner at the first print: {med([e['edge_first_print'] for e in events]):+.3f}")
        tot_q = sum(e['wrong_side_qty_5min'] for e in events)
        print(f"losing-side prints in the first 5 min: {tot_q:.0f} contracts across {sum(1 for e in events if e['wrong_side_qty_5min']>0)} events; size-weighted edge {sum(e['wrong_side_edge']*e['wrong_side_qty_5min'] for e in events)/tot_q if tot_q else 0:+.3f}")
        with open("reports/release-lock.json", "w") as f:
            json.dump({"kind": "release-lock", "created_ms": int(time.time() * 1000), "events": events}, f, indent=1)


if __name__ == "__main__":
    main()
