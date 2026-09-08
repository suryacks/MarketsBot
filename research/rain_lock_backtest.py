"""Historical test of the rain settlement lock.

KXRAIN settles YES if the station records any precipitation during the local day.
Once measurable rain has fallen the outcome is decided, but the market stays open
until midnight — potentially hours of a decided market still trading below $1.

For every KXRAIN market in the dataset:
  * find the first hour with measurable precipitation (IEM ASOS p01i > 0.009")
  * from that moment on, take the best ask in each later hourly candle
  * profit per contract = 1 − ask − fee   (the market settles YES)
Reports how often a decided market was still buyable, the edge, and the PnL of a
simple rule: buy once, at the first quote after the rain, if the edge clears a floor.
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

# KXRAIN city code -> (ASOS station, UTC offset of the local day)
CITY = {
    "ATL": ("ATL", -4), "AUS": ("AUS", -5), "BOS": ("BOS", -4), "CHI": ("ORD", -5), "DAL": ("DFW", -5),
    "DC": ("DCA", -4), "DEN": ("DEN", -6), "EWR": ("EWR", -4), "HOU": ("IAH", -5), "LAX": ("LAX", -7),
    "LV": ("LAS", -7), "MIA": ("MIA", -4), "MIN": ("MSP", -5), "NOLA": ("MSY", -5), "NYC": ("NYC", -4),
    "OKC": ("OKC", -5), "PHIL": ("PHL", -4), "PHX": ("PHX", -7), "SATX": ("SAT", -5), "SEA": ("SEA", -7),
    "SFO": ("SFO", -7), "TTN": ("TTN", -4),
}


def fee(px):
    return math.ceil(0.07 * px * (1 - px) * 100) / 100


def arg(n, d):
    return float(sys.argv[sys.argv.index(n) + 1]) if n in sys.argv else d


def iem_precip(station, start, end):
    """[(utc datetime, hourly precip inches)] from routine + special reports."""
    q = urllib.parse.urlencode({
        "station": station, "data": "p01i", "year1": start.year, "month1": start.month, "day1": start.day,
        "year2": end.year, "month2": end.month, "day2": end.day, "tz": "Etc/UTC", "format": "onlycomma",
        "latlon": "no", "missing": "M", "trace": "0.0001", "direct": "no",
    }) + "&report_type=3"
    url = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?" + q
    for attempt in range(5):
        try:
            with urllib.request.urlopen(url, timeout=180) as r:
                text = r.read().decode()
            break
        except Exception:  # noqa: BLE001
            time.sleep(8 * (attempt + 1))
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
    out.sort()
    return out


def main():
    min_edge = arg("--min-edge", 0.02)
    markets = []
    for f in glob.glob("data/dataset/markets/KXRAIN*.parquet"):
        markets += pq.read_table(f).to_pylist()
    prices = defaultdict(list)
    for f in glob.glob("data/dataset/prices/KXRAIN*.parquet"):
        for p in pq.read_table(f).to_pylist():
            prices[p["ticker"]].append(p)
    for v in prices.values():
        v.sort(key=lambda p: p["ts"])
    markets = [m for m in markets if m["ticker"] in prices]
    print(f"{len(markets)} KXRAIN markets with price paths")
    if not markets:
        return
    lo = datetime.fromtimestamp(min(m["open_ts"] for m in markets), timezone.utc) - timedelta(days=1)
    hi = datetime.fromtimestamp(max(m["close_ts"] for m in markets), timezone.utc) + timedelta(days=1)
    obs = {}
    for city, (stn, _) in CITY.items():
        o = iem_precip(stn, lo, hi)
        if o:
            obs[city] = o
        print(f"  {city} ({stn}): {len(o)} hourly precip reports")
        time.sleep(3)

    rows, yes_markets = [], 0
    for m in markets:
        city = m["ticker"].rsplit("-", 1)[-1]
        if city not in obs:
            continue
        off = CITY[city][1]
        # The ticker carries the market day: KXRAIN-26SEP09-NYC -> local Sep 9.
        parts = m["ticker"].split("-")
        if len(parts) < 3:
            continue
        try:
            d = datetime.strptime(parts[1], "%y%b%d")
        except ValueError:
            continue
        day_start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc) - timedelta(hours=off)
        day_end = day_start + timedelta(hours=24)
        rain_total = sum(v for t, v in obs[city] if day_start <= t < day_end and v > 0)
        first_rain = next((t for t, v in obs[city] if day_start <= t < day_end and v >= 0.01), None)
        if m["result_yes"]:
            yes_markets += 1
        if first_rain is None:
            continue
        # sanity: observation says it rained -> the market should have settled YES
        agree = m["result_yes"]
        after = [p for p in prices[m["ticker"]] if p["ts"] >= first_rain.timestamp() and p.get("ask") is not None]
        if not after:
            continue
        first_q = after[0]
        edge = 1.0 - first_q["ask"] - fee(first_q["ask"])
        hours_left = (m["close_ts"] - first_q["ts"]) / 3600
        rows.append({"ticker": m["ticker"], "city": city, "agree": agree, "settle": m["result_yes"], "ask": first_q["ask"],
                     "edge": edge, "hours_left": hours_left, "rain_in": rain_total,
                     "best_edge": max(1.0 - p["ask"] - fee(p["ask"]) for p in after)})
    print(f"\n{len(rows)} markets where rain was observed during the market day ({yes_markets} settled YES overall)")
    if not rows:
        return
    disagree = [r for r in rows if not r["agree"]]
    print(f"observation vs settlement disagreement: {len(disagree)} of {len(rows)} ({100*len(disagree)/len(rows):.1f}%)")
    for r in disagree[:5]:
        print(f"   MISMATCH {r['ticker']} rain {r['rain_in']:.2f}in but settled NO")
    tradeable = [r for r in rows if r["edge"] >= min_edge]
    print(f"\nstill buyable at >= {min_edge:.2f} edge on the first quote after rain: {len(tradeable)} of {len(rows)}")
    if tradeable:
        pnl = sum((1.0 if r["settle"] else 0.0) - r["ask"] - fee(r["ask"]) for r in tradeable)
        wins = sum(1 for r in tradeable if r["settle"])
        stake_pnl = sum(((1.0 if r["settle"] else 0.0) - r["ask"] - fee(r["ask"])) * (2.0 / max(r["ask"], 0.02)) for r in tradeable)
        print(f"  avg ask {sum(r['ask'] for r in tradeable)/len(tradeable):.3f}, avg edge {sum(r['edge'] for r in tradeable)/len(tradeable):+.3f}, "
              f"avg hours left {sum(r['hours_left'] for r in tradeable)/len(tradeable):.1f}")
        print(f"  wins {wins}/{len(tradeable)} ({100*wins/len(tradeable):.0f}%) | PnL {pnl:+.2f}/contract-set | {stake_pnl:+.2f} at $2 per trade")
        for r in sorted(tradeable, key=lambda r: -r["edge"])[:10]:
            print(f"   {r['ticker']:<26} ask {r['ask']:.2f} edge {r['edge']:+.2f} hours left {r['hours_left']:.1f} settled {'YES' if r['settle'] else 'NO'}")
    print(f"\nbest edge at ANY later quote: median {sorted(r['best_edge'] for r in rows)[len(rows)//2]:+.3f}")


if __name__ == "__main__":
    main()
