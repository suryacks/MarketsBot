"""Riskless bucket arbitrage on recorded weather books.

Each daily-high event has mutually exclusive buckets (B-tickers) plus tails
(T-tickers). Buying YES on every bucket costs Σ asks and pays exactly $1;
buying NO on every bucket costs Σ (1 − bid) and pays N − 1. Whenever
Σ asks < 1 − fees or Σ bids > 1 + fees, the set is riskless profit.
Scans the minute-level books in data/live-weather* for such moments.
"""
import glob
import math
from collections import defaultdict

import pyarrow.parquet as pq


def fee(px):
    return math.ceil(0.07 * px * (1 - px) * 100) / 100


def load_books():
    rows = []
    for f in glob.glob("data/live-weather*/books/**/*.parquet", recursive=True):
        rows += pq.read_table(f).to_pylist()
    rows.sort(key=lambda r: (r["ticker"], r["ts_ms"], r["seq"]))
    best = defaultdict(list)  # ticker -> [(ts, bid, ask, bid_qty, ask_qty)]
    bids, asks = defaultdict(dict), defaultdict(dict)
    for r in rows:
        tk = r["ticker"]
        d = (bids if r["is_bid"] else asks)[tk]
        if r["kind"] == "snap":
            if r["snapshot_start"]:
                bids[tk].clear(); asks[tk].clear()
                d = (bids if r["is_bid"] else asks)[tk]
            d[r["px"]] = r["qty"]
        elif r["kind"] == "delta":
            d[r["px"]] = d.get(r["px"], 0) + r["qty"]
            if d[r["px"]] <= 0:
                d.pop(r["px"], None)
        else:
            if r["qty"] <= 0:
                d.pop(r["px"], None)
            else:
                d[r["px"]] = r["qty"]
        bb = max(bids[tk]) if bids[tk] else None
        ba = min(asks[tk]) if asks[tk] else None
        best[tk].append((r["ts_ms"], bb / 1e4 if bb else None, ba / 1e4 if ba else None, bids[tk].get(bb, 0) / 1e4 if bb else 0, asks[tk].get(ba, 0) / 1e4 if ba else 0))
    return best


def main():
    best = load_books()
    events = defaultdict(list)
    for tk in best:
        parts = tk.split("-")
        if len(parts) == 3 and parts[2].startswith("B"):
            events[parts[0] + "-" + parts[1]].append(tk)
    print(f"{len(best)} tickers, {len(events)} events with buckets")
    total_opps = 0
    for ev, tks in sorted(events.items()):
        if len(tks) < 3:
            continue
        # walk time: at each update of any bucket, evaluate the set using the latest quote of each
        latest = {tk: None for tk in tks}
        timeline = sorted(((ts, tk, bb, ba, bq, aq) for tk in tks for ts, bb, ba, bq, aq in best[tk]), key=lambda x: (x[0], x[1]))
        opps = []
        last_report = 0
        for ts, tk, bb, ba, bq, aq in timeline:
            latest[tk] = (bb, ba, bq, aq)
            if any(v is None for v in latest.values()):
                continue
            asks = [v[1] for v in latest.values()]
            bids = [v[0] for v in latest.values()]
            if all(a is not None for a in asks):
                cost = sum(asks) + sum(fee(a) for a in asks)
                if cost < 0.99:
                    size = min(v[3] for v in latest.values())
                    if ts - last_report > 60_000:
                        opps.append(("BUY ALL YES", ts, 1 - cost, size))
                        last_report = ts
            if all(b is not None for b in bids):
                # buy NO on every bucket: pay Σ(1−bid) + fees, receive N−1
                cost = sum(1 - b for b in bids) + sum(fee(b) for b in bids)
                if (len(bids) - 1) - cost > 0.01:
                    size = min(v[2] for v in latest.values())
                    if ts - last_report > 60_000:
                        opps.append(("BUY ALL NO", ts, (len(bids) - 1) - cost, size))
                        last_report = ts
        if opps:
            total_opps += len(opps)
            print(f"{ev}: {len(opps)} arb moments (≥1 min apart); best profit/set {max(o[2] for o in opps):.3f}, sizes {[round(o[3]) for o in opps[:6]]}")
            for o in opps[:3]:
                print(f"    {o[0]} at {o[1]} profit {o[2]:+.3f}/set size {o[3]:.0f}")
    print(f"\nTOTAL riskless arb moments: {total_opps}")


if __name__ == "__main__":
    main()
