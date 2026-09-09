"""Which station does each Kalshi temperature series actually settle against?

Kalshi publishes `expiration_value` on settled markets: the official daily max or
min the market resolved to. This tries every plausible ASOS station for a city and
keeps the one whose observed extreme matches that official number most often.

Guessing the station is how a settlement-lock strategy quietly goes wrong: the
lock is only safe if the feed we watch is the feed Kalshi settles on. This also
measures the error in the direction that makes a lock WRONG, per series, so the
size of the safety margin is a measured number and not a hope.

Usage: research/venv/bin/python research/station_map.py [--days 45] [--out reports/station-map.json]
"""
import io, json, sys, time, urllib.parse, urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

K = "https://external-api.kalshi.com/trade-api/v2"

# series -> (candidate stations, IEM networks, UTC offset of the local day in September)
E, C, M, P, AZ = -4, -5, -6, -7, -7
CITIES = {
    "NY":   (["NYC", "LGA", "JFK"], ["NY_ASOS"], E),      "CHI":  (["MDW", "ORD"], ["IL_ASOS"], C),
    "MIA":  (["MIA"], ["FL_ASOS"], E),                    "LAX":  (["LAX"], ["CA_ASOS"], P),
    "AUS":  (["AUS"], ["TX_ASOS"], C),                    "PHIL": (["PHL"], ["PA_ASOS"], E),
    "DEN":  (["DEN"], ["CO_ASOS"], M),
    "TSDF": (["SDF"], ["KY_ASOS"], E),                    "TTTN": (["TTN"], ["NJ_ASOS"], E),
    "TEWR": (["EWR"], ["NJ_ASOS"], E),                    "TSAN": (["SAN"], ["CA_ASOS"], P),
    "TOKC": (["OKC"], ["OK_ASOS"], C),                    "THOU": (["HOU", "IAH"], ["TX_ASOS"], C),
    "TSATX":(["SAT"], ["TX_ASOS"], C),                    "TDAL": (["DAL", "DFW"], ["TX_ASOS"], C),
    "TMIN": (["MSP"], ["MN_ASOS"], C),                    "TATL": (["ATL"], ["GA_ASOS"], E),
    "TPHX": (["PHX"], ["AZ_ASOS"], AZ),                   "TBOS": (["BOS"], ["MA_ASOS"], E),
    "TSEA": (["SEA"], ["WA_ASOS"], P),                    "TDC":  (["DCA", "IAD"], ["DC_ASOS", "VA_ASOS"], E),
    "TSFO": (["SFO"], ["CA_ASOS"], P),                    "TNOLA":(["MSY"], ["LA_ASOS"], C),
    "TLV":  (["LAS"], ["NV_ASOS"], P),                    "TNYC": (["NYC", "LGA"], ["NY_ASOS"], E),
    "TMIA": (["MIA"], ["FL_ASOS"], E),                    "TLAX": (["LAX"], ["CA_ASOS"], P),
    "TAUS": (["AUS"], ["TX_ASOS"], C),                    "TPHIL":(["PHL"], ["PA_ASOS"], E),
    "TDEN": (["DEN"], ["CO_ASOS"], M),                    "TCHI": (["MDW", "ORD"], ["IL_ASOS"], C),
}


def arg(n, d):
    return type(d)(sys.argv[sys.argv.index(n) + 1]) if n in sys.argv else d


def get(u, tries=5):
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "marketsbot-research"}), timeout=40) as r:
                d = json.load(r)
            time.sleep(0.25)
            return d
        except Exception:  # noqa: BLE001
            time.sleep(2 * (i + 1))
    return {}


_obs_cache = {}


def iem(network, station, start, end):
    key = (network, station, start.date(), end.date())
    if key in _obs_cache:
        return _obs_cache[key]
    q = urllib.parse.urlencode({
        "station": station, "network": network, "data": "tmpf", "year1": start.year, "month1": start.month,
        "day1": start.day, "year2": end.year, "month2": end.month, "day2": end.day, "tz": "Etc/UTC",
        "format": "onlycomma", "latlon": "no", "missing": "M", "trace": "T", "direct": "no",
    }) + "&report_type=2&report_type=3"
    url = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?" + q
    text = ""
    for i in range(4):
        try:
            with urllib.request.urlopen(url, timeout=300) as r:
                text = r.read().decode()
            break
        except Exception:  # noqa: BLE001
            time.sleep(8 * (i + 1))
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
    _obs_cache[key] = out
    return out


def settled_by_day(series, since):
    """{local date -> official value} from Kalshi's own settlement field."""
    d = get(f"{K}/markets?series_ticker={series}&status=settled&min_close_ts={since}&limit=1000&mve_filter=exclude")
    by = {}
    for m in d.get("markets", []):
        v = m.get("expiration_value")
        if not v:
            continue
        parts = m["ticker"].split("-")
        if len(parts) < 3:
            continue
        try:
            by[datetime.strptime(parts[1], "%y%b%d").date()] = float(v)
        except ValueError:
            continue
    return by


def main():
    days = arg("--days", 45)
    out_path = arg("--out", "reports/station-map.json")
    since = int(time.time()) - days * 86400
    results = []
    for kind in ("HIGH", "LOW"):
        for city, (stations, networks, off) in CITIES.items():
            series = f"KX{kind}{city}" if kind == "HIGH" else f"KXLOW{city}"
            # Kalshi's low series are KXLOWT*, highs are KXHIGH* / KXHIGHT*
            if kind == "LOW":
                series = "KXLOWT" + (city[1:] if city.startswith("T") else city)
            by_day = settled_by_day(series, since)
            if not by_day:
                continue
            lo_d, hi_d = min(by_day), max(by_day)
            best = None
            for st in stations:
                for net in networks:
                    obs = iem(net, st, datetime.combine(lo_d, datetime.min.time()) - timedelta(days=2),
                              datetime.combine(hi_d, datetime.min.time()) + timedelta(days=2))
                    if not obs:
                        continue
                    ext = defaultdict(list)
                    for t, v in obs:
                        ext[(t + timedelta(hours=off)).date()].append(v)
                    diffs = []
                    for day, official in by_day.items():
                        vals = ext.get(day)
                        if not vals:
                            continue
                        # Round as the settlement does: Kalshi resolves to whole degrees.
                        mine = round(max(vals)) if kind == "HIGH" else round(min(vals))
                        diffs.append(mine - official)
                    if len(diffs) < 5:
                        continue
                    exact = sum(1 for d_ in diffs if abs(d_) < 0.51)
                    # A lock is wrong when our observation is MORE extreme than the official
                    # value: hotter than the official max, or colder than the official min.
                    unsafe = [d_ for d_ in diffs if (d_ > 0.5 if kind == "HIGH" else d_ < -0.5)]
                    cand = {"series": series, "kind": kind, "station": st, "network": net, "utc_offset": off,
                            "days": len(diffs), "exact_pct": 100 * exact / len(diffs),
                            "unsafe_n": len(unsafe), "unsafe_pct": 100 * len(unsafe) / len(diffs),
                            "worst_unsafe": max((abs(x) for x in unsafe), default=0.0)}
                    if best is None or cand["exact_pct"] > best["exact_pct"]:
                        best = cand
            if best:
                results.append(best)
                print(f"{best['series']:<14} -> K{best['station']:<4} {best['days']:>3}d  match {best['exact_pct']:>5.1f}%  "
                      f"lock-wrong {best['unsafe_n']}/{best['days']} ({best['unsafe_pct']:.2f}%)  worst {best['worst_unsafe']:.1f}F", flush=True)
    with open(out_path, "w") as f:
        json.dump({"kind": "station-map", "created_ms": int(time.time() * 1000), "rows": results}, f, indent=1)
    ok = [r for r in results if r["exact_pct"] >= 90 and r["unsafe_pct"] <= 1.0]
    print(f"\n{len(results)} series validated; {len(ok)} are safe to lock (>=90% station match, <=1% lock-wrong)")
    print(f"written to {out_path}")


if __name__ == "__main__":
    main()
