"""Does our observation feed agree with Kalshi's ACTUAL settled value?

Kalshi publishes `expiration_value` on settled markets — the number the market
resolved against. For temperature markets that is the day's official maximum.
This compares it to the maximum hourly ASOS observation we can see live, so we
can measure exactly how often "the observation says it is decided" would have
been wrong, and with what margin it becomes safe.

Without this number, any settlement-lock strategy is faith. With it, we know the
error rate and can size the safety margin.

Usage: research/venv/bin/python research/settlement_truth.py [--days 45]
"""
import io
import json
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

K = "https://external-api.kalshi.com/trade-api/v2"
# Kalshi series -> (IEM network, station, UTC offset of the local day)
SITES = {
    "KXHIGHNY": ("NY_ASOS", "NYC", -4), "KXHIGHCHI": ("IL_ASOS", "MDW", -5), "KXHIGHMIA": ("FL_ASOS", "MIA", -4),
    "KXHIGHLAX": ("CA_ASOS", "LAX", -7), "KXHIGHAUS": ("TX_ASOS", "AUS", -5), "KXHIGHPHIL": ("PA_ASOS", "PHL", -4),
    "KXHIGHDEN": ("CO_ASOS", "DEN", -6),
}


def arg(n, d):
    return type(d)(sys.argv[sys.argv.index(n) + 1]) if n in sys.argv else d


def get(u, tries=6):
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "marketsbot-research"}), timeout=40) as r:
                d = json.load(r)
            time.sleep(0.3)
            return d
        except Exception:  # noqa: BLE001
            time.sleep(2 * (i + 1))
    return {}


def iem(network, station, start, end):
    """[(utc dt, temp F)] hourly + special reports, explicit network."""
    q = urllib.parse.urlencode({
        "station": station, "network": network, "data": "tmpf", "year1": start.year, "month1": start.month, "day1": start.day,
        "year2": end.year, "month2": end.month, "day2": end.day, "tz": "Etc/UTC", "format": "onlycomma",
        "latlon": "no", "missing": "M", "trace": "T", "direct": "no",
    }) + "&report_type=2&report_type=3"
    url = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?" + q
    for i in range(4):
        try:
            with urllib.request.urlopen(url, timeout=240) as r:
                text = r.read().decode()
            break
        except Exception:  # noqa: BLE001
            time.sleep(10 * (i + 1))
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


def main():
    days = arg("--days", 45)
    since = int(time.time()) - days * 86400
    rows = []
    for series, (net, stn, off) in SITES.items():
        d = get(f"{K}/markets?series_ticker={series}&status=settled&min_close_ts={since}&limit=1000&mve_filter=exclude")
        ms = [m for m in d.get("markets", []) if m.get("expiration_value")]
        # one settled value per market day
        by_day = {}
        for m in ms:
            parts = m["ticker"].split("-")
            if len(parts) < 3:
                continue
            try:
                day = datetime.strptime(parts[1], "%y%b%d").date()
            except ValueError:
                continue
            try:
                by_day[day] = float(m["expiration_value"])
            except ValueError:
                continue
        if not by_day:
            print(f"{series}: no settled values"); continue
        lo, hi = min(by_day), max(by_day)
        obs = iem(net, stn, datetime.combine(lo, datetime.min.time()) - timedelta(days=2), datetime.combine(hi, datetime.min.time()) + timedelta(days=2))
        maxes = defaultdict(lambda: -999.0)
        for t, v in obs:
            local_day = (t + timedelta(hours=off)).date()
            maxes[local_day] = max(maxes[local_day], v)
        agree = diffs = 0
        worst = []
        for day, settled in sorted(by_day.items()):
            om = maxes.get(day)
            if om is None or om < -900:
                continue
            diffs += 1
            d_ = om - settled          # observation minus official
            if abs(d_) < 0.51:
                agree += 1
            else:
                worst.append((day, om, settled, d_))
            rows.append({"series": series, "day": str(day), "obs_max": om, "settled": settled, "diff": d_})
        print(f"{series} ({stn}): {diffs} days compared, {agree} matched within 0.5F ({100*agree/max(diffs,1):.0f}%)")
        for w in sorted(worst, key=lambda x: -abs(x[3]))[:3]:
            print(f"    {w[0]} observed {w[1]:.0f}F vs settled {w[2]:.0f}F  (obs {'above' if w[3]>0 else 'below'} by {abs(w[3]):.0f}F)")
    if not rows:
        return
    ds = [r["diff"] for r in rows]
    over = [d for d in ds if d > 0.5]
    under = [d for d in ds if d < -0.5]
    print(f"\nTOTAL {len(ds)} market-days")
    print(f"  observation ABOVE official by >0.5F: {len(over)} ({100*len(over)/len(ds):.1f}%)   <-- these make a YES-lock WRONG")
    print(f"  observation BELOW official by >0.5F: {len(under)} ({100*len(under)/len(ds):.1f}%)  <-- safe direction (official even higher)")
    print(f"  exact-ish match: {len(ds)-len(over)-len(under)} ({100*(len(ds)-len(over)-len(under))/len(ds):.1f}%)")
    for margin in (0, 1, 2, 3):
        bad = sum(1 for d in ds if d > margin + 0.5)
        print(f"  with a {margin}F safety margin, observation-decided-YES would be wrong {bad}/{len(ds)} times ({100*bad/len(ds):.2f}%)")
    with open("reports/settlement-truth.json", "w") as f:
        json.dump({"kind": "settlement-truth", "created_ms": int(time.time() * 1000), "rows": rows}, f, indent=1)


if __name__ == "__main__":
    main()
