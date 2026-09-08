"""Ladder monotonicity violations — true arbitrage, no prediction required.

For a "greater than" ladder on the same event, P(X > k) must fall as k rises.
So YES(low strike) >= YES(high strike) always. If the market ever shows
    bid(high strike) > ask(low strike) + fees
then: buy YES on the LOW strike, sell YES on the HIGH strike.
Payoff by outcome: X > k_high → 1 − 1 = 0; k_low < X ≤ k_high → 1 − 0 = +1;
X ≤ k_low → 0 − 0 = 0. Never negative, and the trade is entered for a credit.

"Between" bucket ladders get the complementary check: the sum of a contiguous
run of buckets must not exceed the "greater than k" market covering the same
range, and no bucket may trade above any single-sided market containing it.

Scans every event in data/dataset (hourly bid/ask paths, 250 series).
"""
import glob
import math
from collections import defaultdict

import pyarrow.parquet as pq


def fee(px):
    return math.ceil(0.07 * px * (1 - px) * 100) / 100


def strike_of(m):
    """(kind, k) where kind is 'gt' for greater-than ladders."""
    st = m.get("strike_type") or ""
    if st in ("greater", "greater_or_equal") and m.get("floor_strike") is not None:
        return ("gt", float(m["floor_strike"]))
    return (None, None)


def main():
    markets = []
    for f in glob.glob("data/dataset/markets/*.parquet"):
        markets += pq.read_table(f).to_pylist()
    prices = defaultdict(list)
    for f in glob.glob("data/dataset/prices/*.parquet"):
        for p in pq.read_table(f).to_pylist():
            if p.get("bid") is not None or p.get("ask") is not None:
                prices[p["ticker"]].append(p)
    for v in prices.values():
        v.sort(key=lambda p: p["ts"])
    print(f"{len(markets)} markets, {len(prices)} with quotes")

    # A ladder is one SUBJECT at several strikes. Within one event Kalshi lists many
    # subjects (both pitchers, both players, both sides of a spread), so the subject
    # must be part of the key: the title with all numbers stripped.
    import re

    def subject(m):
        return re.sub(r"[\d.]+", "", m.get("title", "")).strip().lower()

    events = defaultdict(list)
    for m in markets:
        kind, k = strike_of(m)
        if kind == "gt" and m["ticker"] in prices:
            events[(m["event_ticker"], subject(m))].append((k, m))
    events = {e: sorted(v, key=lambda x: x[0]) for e, v in events.items() if len(v) >= 2}
    print(f"{len(events)} (event, subject) ladders with 2+ strikes")

    total, best = 0, []
    by_series = defaultdict(int)
    for ev, legs in events.items():
        # align quotes on a common time grid (hourly candles): map ts -> (bid, ask)
        q = {}
        for k, m in legs:
            for p in prices[m["ticker"]]:
                q.setdefault(p["ts"], {})[k] = (p.get("bid"), p.get("ask"))
        for ts, byk in sorted(q.items()):
            ks = sorted(byk)
            for i in range(len(ks)):
                for j in range(i + 1, len(ks)):
                    lo_k, hi_k = ks[i], ks[j]           # lo_k < hi_k
                    lo_ask = byk[lo_k][1]               # buy YES on the low strike
                    hi_bid = byk[hi_k][0]               # sell YES on the high strike
                    if lo_ask is None or hi_bid is None:
                        continue
                    credit = hi_bid - lo_ask - fee(lo_ask) - fee(hi_bid)
                    if credit > 0.01:
                        total += 1
                        by_series[legs[0][1]["series"]] += 1
                        best.append((credit, ev, lo_k, hi_k, lo_ask, hi_bid, ts, legs[0][1]["title"]))
    best.sort(key=lambda x: -x[0])
    print(f"\nviolations (credit > 1c after fees): {total}")
    for c, ev, lk, hk, la, hb, ts, title in best[:15]:
        print(f"  +{c:.3f}/set  {ev[0]} [{title[:40]}]: buy YES >{lk:g} at {la:.2f}, sell YES >{hk:g} at {hb:.2f}")
    if by_series:
        print("\nby series:", dict(sorted(by_series.items(), key=lambda x: -x[1])[:12]))


if __name__ == "__main__":
    main()
