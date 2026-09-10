"""Replay the deployed lock rule over settled markets and count how often it was wrong.

The earlier study compared observations to official values and reported a discrepancy rate.
That is not the same question. What matters is the rule the bot actually applies -- with its
tolerance, its rounding, and its one-degree settlement gap -- so this replays exactly that
decision against markets whose answers are known, and reports the rate at which it would
have sold a bucket that went on to win.

Observations come from IEM in whole degrees F, which is both the unit Kalshi settles in and
the feed the bot now reads.
"""
import io, json, sys, time, urllib.parse, urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
sys.path.insert(0, "research")
from kalshi_auth import get  # noqa: E402

TOLERANCE_F = 0.5
SITES = {}
for r in json.load(open("reports/station-map.json"))["rows"]:
    if r["worst_unsafe"] <= 1.5:
        SITES[r["series"]] = (r["network"], r["station"], r["utc_offset"])


def iem(net, stn, d0, d1):
    q = urllib.parse.urlencode({"station": stn, "network": net, "data": "tmpf", "year1": d0.year, "month1": d0.month,
        "day1": d0.day, "year2": d1.year, "month2": d1.month, "day2": d1.day, "tz": "Etc/UTC",
        "format": "onlycomma", "latlon": "no", "missing": "M", "trace": "T", "direct": "no"}) + "&report_type=2&report_type=3"
    for i in range(3):
        try:
            with urllib.request.urlopen("https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?" + q, timeout=240) as r:
                text = r.read().decode()
            break
        except Exception:  # noqa: BLE001
            time.sleep(5 * (i + 1))
    else:
        return []
    out = []
    for line in io.StringIO(text):
        p = line.strip().split(",")
        if len(p) < 3 or p[0] == "station":
            continue
        try:
            out.append((datetime.strptime(p[1][:16], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc), float(p[2])))
        except ValueError:
            continue
    return sorted(out)


def bounds(m):
    """(lo, hi) the official value must land in for YES. -999/999 mean unbounded."""
    t = m["ticker"].rsplit("-", 1)[-1]
    st = (m.get("strike_type") or "").lower()
    if t.startswith("B"):
        c = float(t[1:])
        return float(int(c)), float(int(c) + 1)
    if st == "greater":
        return float(m["floor_strike"]) + 1, 999.0
    if st == "greater_or_equal":
        return float(m["floor_strike"]), 999.0
    if st == "less":
        return -999.0, float(m["cap_strike"]) - 1
    if st == "less_or_equal":
        return -999.0, float(m["cap_strike"])
    return None


def main():
    days = int(sys.argv[sys.argv.index("--days") + 1]) if "--days" in sys.argv else 40
    since = int(time.time()) - days * 86400
    fired = wrong = 0
    rows = []
    for series, (net, stn, off) in sorted(SITES.items()):
        low = series.startswith("KXLOW")
        d = get("/markets", {"series_ticker": series, "status": "settled", "min_close_ts": since, "limit": 1000, "mve_filter": "exclude"})
        # `result` is the ground truth. `expiration_value` is a string like "58 to 59" on
        # bucket markets, so it cannot be parsed as the official number for those.
        ms = [m for m in d.get("markets", []) if m.get("result") in ("yes", "no")]
        if not ms:
            continue
        by_day = defaultdict(list)
        for m in ms:
            try:
                day = datetime.strptime(m["ticker"].split("-")[1], "%y%b%d").date()
            except (ValueError, IndexError):
                continue
            by_day[day].append(m)
        lo_d, hi_d = min(by_day), max(by_day)
        obs = iem(net, stn, datetime.combine(lo_d, datetime.min.time()) - timedelta(days=2),
                  datetime.combine(hi_d, datetime.min.time()) + timedelta(days=2))
        ext = defaultdict(list)
        for t, v in obs:
            ext[(t + timedelta(hours=off)).date()].append(v)
        for day, mkts in sorted(by_day.items()):
            vals = ext.get(day)
            if not vals:
                continue
            observed = min(vals) if low else max(vals)
            for m in mkts:
                b = bounds(m)
                if not b:
                    continue
                lo, hi = b
                # exactly the rule the bot applies
                if low:
                    warmest = observed + TOLERANCE_F
                    sell = warmest <= lo - 1.0
                else:
                    coolest = observed - TOLERANCE_F
                    sell = coolest >= hi + 1.0 and hi < 999.0
                if not sell:
                    continue
                fired += 1
                won = m["result"] == "yes"
                if won:
                    wrong += 1
                    rows.append({"ticker": m["ticker"], "observed": observed, "lo": lo, "hi": hi,
                                 "official": m.get("expiration_value")})
        print(f"  {series:<14} checked", flush=True)
    print(f"\nrule fired on {fired} settled markets")
    print(f"WRONG (sold a bucket that won): {wrong}  = {100*wrong/max(fired,1):.2f}%")
    for r in rows[:10]:
        print(f"   {r['ticker']:<28} observed {r['observed']:>6.1f} bucket [{r['lo']},{r['hi']}] official {r['official']}")
    json.dump({"kind": "lock-rule", "fired": fired, "wrong": wrong, "rows": rows}, open("reports/lock-rule.json", "w"), indent=1)


if __name__ == "__main__":
    main()
