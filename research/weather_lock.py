"""Settlement lock on daily high-temperature markets.

Once the day's running maximum (hourly ASOS observations, free from Iowa
Environmental Mesonet) has exceeded a "≥ X°" strike, that market is decided YES
(the official daily max can only be ≥ the hourly max). Once the running max has
passed *above* a "between" bucket, that bucket is decided NO. Any price that
still offers room after that moment is a locked-in trade.

Uses the price paths already in data/dataset (hourly candles) so no new market
data is fetched. Reports: opportunities, average edge after fees, fraction of
the time the market had already caught up, and a simple $2-per-trade PnL.

Usage: research/venv/bin/python research/weather_lock.py [--min-edge 0.02]
"""
import io
import math
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq

# Kalshi series -> (IEM ASOS station, IANA tz, UTC offset hours used for "market day")
STATIONS = {
    "KXHIGHNY": ("NYC", -4), "KXHIGHCHI": ("MDW", -5), "KXHIGHMIA": ("MIA", -4), "KXHIGHLAX": ("LAX", -7),
    "KXHIGHAUS": ("AUS", -5), "KXHIGHPHIL": ("PHL", -4), "KXHIGHDEN": ("DEN", -6),
}


def arg(name, default):
    return float(sys.argv[sys.argv.index(name) + 1]) if name in sys.argv else default


def fee(px):
    return math.ceil(0.07 * px * (1 - px) * 100) / 100


def iem_hourly(station, start, end):
    """{utc datetime: temp F} for hourly-ish ASOS obs."""
    q = urllib.parse.urlencode({
        "station": station, "data": "tmpf", "year1": start.year, "month1": start.month, "day1": start.day,
        "year2": end.year, "month2": end.month, "day2": end.day, "tz": "Etc/UTC", "format": "onlycomma",
        "latlon": "no", "missing": "M", "trace": "T", "direct": "no", "report_type": "3",
    })
    url = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?" + q
    with urllib.request.urlopen(url, timeout=120) as r:
        text = r.read().decode()
    out = {}
    for line in io.StringIO(text):
        parts = line.strip().split(",")
        if len(parts) < 3 or parts[0] == "station":
            continue
        try:
            t = datetime.strptime(parts[1], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            out[t] = float(parts[2])
        except ValueError:
            continue
    return out


def main():
    min_edge = arg("--min-edge", 0.02)
    markets = pq.read_table("data/dataset/markets").to_pylist() if False else None
    import glob
    markets = []
    for f in glob.glob("data/dataset/markets/KXHIGH*.parquet"):
        markets += pq.read_table(f).to_pylist()
    prices = defaultdict(list)
    for f in glob.glob("data/dataset/prices/KXHIGH*.parquet"):
        for p in pq.read_table(f).to_pylist():
            prices[p["ticker"]].append(p)
    markets = [m for m in markets if m["series"] in STATIONS]
    print(f"{len(markets)} weather markets with price paths")
    if not markets:
        return
    t0 = min(m["open_ts"] for m in markets)
    t1 = max(m["close_ts"] for m in markets)
    obs = {}
    for s, (stn, _) in STATIONS.items():
        try:
            obs[s] = iem_hourly(stn, datetime.fromtimestamp(t0, timezone.utc) - timedelta(days=1), datetime.fromtimestamp(t1, timezone.utc) + timedelta(days=1))
            print(f"  {s}: {len(obs[s])} hourly obs from {stn}")
        except Exception as e:  # noqa: BLE001
            print(f"  {s}: obs failed {e}")

    opp = []  # (series, ticker, kind, hour_before_close, px, edge)
    caught_up = 0
    decided = 0
    for m in markets:
        s = m["series"]
        if s not in obs or not prices.get(m["ticker"]):
            continue
        _, off = STATIONS[s]
        # market day = local date of (close - 6h)
        day_local = datetime.fromtimestamp(m["close_ts"], timezone.utc) + timedelta(hours=off) - timedelta(hours=6)
        day_start_utc = datetime(day_local.year, day_local.month, day_local.day, tzinfo=timezone.utc) - timedelta(hours=off)
        day_end_utc = day_start_utc + timedelta(hours=24)
        tick = m["ticker"].rsplit("-", 1)[-1]
        st = m["strike_type"]
        if tick.startswith("B"):
            c = float(tick[1:]); lo, hi = math.floor(c), math.ceil(c)
            kind = "between"
        elif st in ("greater", "greater_or_equal") and m["floor_strike"] is not None:
            lo = math.floor(m["floor_strike"]) + (1 if st == "greater" else 0); hi = 999; kind = "greater"
        else:
            continue
        path = sorted(prices[m["ticker"]], key=lambda p: p["ts"])
        # running max of obs during the local day, evaluated at each hourly candle
        for p in path:
            t = datetime.fromtimestamp(p["ts"], timezone.utc)
            if t < day_start_utc or t > day_end_utc:
                continue
            run_max = max((v for k, v in obs[s].items() if day_start_utc <= k <= t), default=None)
            if run_max is None:
                continue
            bid, ask = p.get("bid"), p.get("ask")
            if kind == "greater" and run_max >= lo:
                decided += 1
                if ask is None or ask >= 0.97:
                    caught_up += 1
                    continue
                edge = 1.0 - ask - fee(ask)
                if edge >= min_edge:
                    opp.append((s, m["ticker"], "YES locked", (m["close_ts"] - p["ts"]) / 3600, ask, edge, m["result_yes"]))
            elif kind == "between" and run_max > hi:
                decided += 1
                if bid is None or bid <= 0.03:
                    caught_up += 1
                    continue
                edge = bid - fee(bid)  # buy NO at 1-bid, pays 1
                if edge >= min_edge:
                    opp.append((s, m["ticker"], "NO locked", (m["close_ts"] - p["ts"]) / 3600, 1 - bid, edge, m["result_yes"]))
    # one trade per market (first opportunity)
    first = {}
    for o in sorted(opp, key=lambda o: -o[3]):
        first.setdefault(o[1], o)
    trades = list(first.values())
    print(f"\ndecided-moments observed: {decided}, market already ≥0.97/≤0.03: {caught_up} ({100*caught_up/max(decided,1):.0f}%)")
    print(f"tradeable moments (edge ≥ {min_edge:.2f}): {len(opp)} across {len(trades)} markets")
    if trades:
        wins = sum(1 for t in trades if (t[2] == 'YES locked') == t[6])
        pnl = sum(((1 - t[4]) if (t[2] == 'YES locked') == t[6] else -t[4]) - fee(t[4]) for t in trades)
        stake_pnl = sum((2.0 / t[4]) * ((((1 - t[4]) if (t[2] == 'YES locked') == t[6] else -t[4])) - fee(t[4])) for t in trades)
        print(f"first-opportunity trades: {len(trades)}, would-have-won {wins} ({100*wins/len(trades):.0f}%), avg edge {sum(t[5] for t in trades)/len(trades):+.3f}/contract, "
              f"PnL {pnl:+.2f} per 1-lot, {stake_pnl:+.2f} at $2/trade")
        by_series = defaultdict(list)
        for t in trades:
            by_series[t[0]].append(t)
        for s, ts in sorted(by_series.items()):
            w = sum(1 for t in ts if (t[2] == 'YES locked') == t[6])
            print(f"   {s}: {len(ts)} trades, {w} wins, avg edge {sum(t[5] for t in ts)/len(ts):+.3f}, avg hours before close {sum(t[3] for t in ts)/len(ts):.1f}")
        losers = [t for t in trades if (t[2] == 'YES locked') != t[6]]
        for t in losers[:8]:
            print("   LOSS (obs max vs official max disagree?):", t[1], t[2], f"px {t[4]:.2f}")


if __name__ == "__main__":
    main()
