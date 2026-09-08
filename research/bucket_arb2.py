"""Partition-aware riskless arbitrage on recorded weather books.

Uses each market's strike metadata (from the API) to assemble the FULL
partition of an event — low tail (less/less_or_equal), between-buckets, high
tail (greater/greater_or_equal) — and verifies it covers the whole-degree
line without gaps or overlaps. Only then: Σ asks < 1 − fees ⇒ buy every
leg (pays exactly $1); Σ bids > 1 + fees ⇒ sell every leg.
"""
import glob
import json
import math
import time
import urllib.request
from collections import defaultdict

import pyarrow.parquet as pq

K = "https://external-api.kalshi.com/trade-api/v2"


def fee(px):
    return math.ceil(0.07 * px * (1 - px) * 100) / 100


def get(u, tries=8):
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "marketsbot-research"}), timeout=30) as r:
                d = json.load(r)
            time.sleep(0.6)
            return d
        except Exception as e:  # noqa: BLE001
            time.sleep(3 * (i + 1))
            last = e
    raise last


def load_best():
    rows = []
    for f in glob.glob("data/live-weather*/books/**/*.parquet", recursive=True):
        rows += pq.read_table(f).to_pylist()
    rows.sort(key=lambda r: (r["ticker"], r["ts_ms"], r["seq"]))
    best = defaultdict(list)
    bids, asks = defaultdict(dict), defaultdict(dict)
    for r in rows:
        tk = r["ticker"]
        d = (bids if r["is_bid"] else asks)[tk]
        if r["kind"] == "snap":
            if r["snapshot_start"]:
                bids[tk].clear(); asks[tk].clear(); d = (bids if r["is_bid"] else asks)[tk]
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


def interval(m):
    """Whole-degree interval [lo, hi] the market pays on, or None."""
    st, fl, cap = m.get("strike_type"), m.get("floor_strike"), m.get("cap_strike")
    if st == "between" and fl is not None and cap is not None:
        return (math.ceil(fl), math.floor(cap))
    if st == "greater" and fl is not None:
        return (math.floor(fl) + 1, 10**6)
    if st == "greater_or_equal" and fl is not None:
        return (math.ceil(fl), 10**6)
    if st == "less" and cap is not None:
        return (-10**6, math.ceil(cap) - 1)
    if st == "less_or_equal" and cap is not None:
        return (-10**6, math.floor(cap))
    if st == "less" and fl is not None:
        return (-10**6, math.ceil(fl) - 1)
    if st == "less_or_equal" and fl is not None:
        return (-10**6, math.floor(fl))
    return None


def main():
    best = load_best()
    events = defaultdict(set)
    for tk in best:
        p = tk.split("-")
        if len(p) == 3:
            events[p[0] + "-" + p[1]].add(tk)
    print(f"{len(events)} events in recorded books")
    total = 0
    for ev, tks in sorted(events.items()):
        meta = get(f"{K}/markets?event_ticker={ev}&limit=50").get("markets", [])
        ivs = {}
        for m in meta:
            iv = interval(m)
            if iv:
                ivs[m["ticker"]] = iv
        # partition check: sorted by lo, contiguous, starting at -inf and ending at +inf
        legs = sorted(ivs.items(), key=lambda x: x[1][0])
        ok = bool(legs) and legs[0][1][0] == -10**6 and legs[-1][1][1] == 10**6 and all(legs[i][1][1] + 1 == legs[i + 1][1][0] for i in range(len(legs) - 1))
        have = [t for t, _ in legs if t in best]
        if not ok or len(have) < len(legs):
            print(f"{ev}: partition {'OK' if ok else 'INCOMPLETE'} ({len(legs)} legs: {[(t.rsplit('-',1)[-1], iv) for t, iv in legs][:8]}), books for {len(have)}/{len(legs)} — skipped")
            continue
        latest = {t: None for t, _ in legs}
        timeline = sorted(((ts, tk, bb, ba, bq, aq) for tk in latest for ts, bb, ba, bq, aq in best[tk]), key=lambda x: (x[0], x[1]))
        opps, last = [], 0
        for ts, tk, bb, ba, bq, aq in timeline:
            latest[tk] = (bb, ba, bq, aq)
            if any(v is None for v in latest.values()):
                continue
            asks = [v[1] for v in latest.values()]
            bids = [v[0] for v in latest.values()]
            if all(a is not None for a in asks):
                cost = sum(asks) + sum(fee(a) for a in asks)
                if cost < 0.995 and ts - last > 60_000:
                    opps.append(("BUY ALL", ts, 1 - cost, min(v[3] for v in latest.values()))); last = ts
            if all(b is not None for b in bids):
                cost = sum(1 - b for b in bids) + sum(fee(b) for b in bids)
                if (len(bids) - 1) - cost > 0.005 and ts - last > 60_000:
                    opps.append(("SELL ALL", ts, (len(bids) - 1) - cost, min(v[2] for v in latest.values()))); last = ts
        total += len(opps)
        print(f"{ev}: partition OK ({len(legs)} legs), {len(opps)} riskless moments" + (f"; best +{max(o[2] for o in opps):.3f}/set, sizes {[round(o[3]) for o in opps[:5]]}" if opps else ""))
    print(f"\nTOTAL riskless moments over full partitions: {total}")


if __name__ == "__main__":
    main()
