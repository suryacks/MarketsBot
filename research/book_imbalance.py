"""Order-book imbalance on the recorded BTC 15-minute books (data/live).

Signal: I = (bid depth − ask depth) / (bid depth + ask depth) over the top N
levels. Question: does I at time t predict the mid change over the next
10 / 30 / 60 s, and is the effect larger than the spread + fee?

Replays BookRow files (snapshots/deltas) for KXBTC15M tickers, samples every
5 s, and reports correlation and a threshold-rule PnL (enter at the touch when
|I| > k, exit at mid after H seconds — an approximation; fees included).
"""
import glob
import math
from collections import defaultdict

import pyarrow.parquet as pq


def fee(px):
    return math.ceil(0.07 * px * (1 - px) * 100) / 100


def main():
    rows = []
    for f in sorted(glob.glob("data/live/books/**/*.parquet", recursive=True)):
        t = pq.read_table(f).to_pylist()
        rows += [r for r in t if r["ticker"].startswith("KXBTC15M")]
    rows.sort(key=lambda r: (r["ticker"], r["ts_ms"], r["seq"]))
    print(f"{len(rows)} book rows")
    samples = []  # (ticker, ts, mid, bid, ask, I1, I3)
    by_ticker = defaultdict(list)
    for r in rows:
        by_ticker[r["ticker"]].append(r)
    for tk, rs in by_ticker.items():
        bids, asks = {}, {}
        last_sample = 0
        for r in rs:
            d = bids if r["is_bid"] else asks
            if r["kind"] == "snap":
                if r["snapshot_start"]:
                    bids.clear(); asks.clear(); d = bids if r["is_bid"] else asks
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
            if r["ts_ms"] - last_sample < 5000 or not bids or not asks:
                continue
            last_sample = r["ts_ms"]
            bb, ba = max(bids), min(asks)
            if ba <= bb:
                continue
            top_b = sorted(bids, reverse=True)[:3]; top_a = sorted(asks)[:3]
            b1, a1 = bids[bb], asks[ba]
            b3, a3 = sum(bids[p] for p in top_b), sum(asks[p] for p in top_a)
            samples.append((tk, r["ts_ms"], (bb + ba) / 2e4, bb / 1e4, ba / 1e4, (b1 - a1) / (b1 + a1), (b3 - a3) / (b3 + a3)))
    print(f"{len(samples)} samples (5 s) across {len(by_ticker)} markets")
    # future mid change
    by_tk = defaultdict(list)
    for s in samples:
        by_tk[s[0]].append(s)
    for horizon in (10, 30, 60):
        xs1, xs3, ys = [], [], []
        for tk, ss in by_tk.items():
            j = 0
            for i, s in enumerate(ss):
                while j < len(ss) and ss[j][1] < s[1] + horizon * 1000:
                    j += 1
                if j >= len(ss):
                    break
                xs1.append(s[5]); xs3.append(s[6]); ys.append(ss[j][2] - s[2])
        def corr(x, y):
            n = len(x); mx, my = sum(x) / n, sum(y) / n
            sx = sum((a - mx) ** 2 for a in x) ** 0.5; sy = sum((b - my) ** 2 for b in y) ** 0.5
            return sum((a - mx) * (b - my) for a, b in zip(x, y)) / (sx * sy) if sx and sy else 0
        print(f"\nhorizon {horizon}s: n={len(ys)}  corr(I_top1, Δmid)={corr(xs1, ys):+.3f}  corr(I_top3, Δmid)={corr(xs3, ys):+.3f}")
        for k in (0.3, 0.5, 0.7):
            # rule: I3 > k → buy YES at ask, exit at mid after horizon; I3 < -k → buy NO (sell YES at bid)
            pnl, n = 0.0, 0
            for tk, ss in by_tk.items():
                j = 0
                for i, s in enumerate(ss):
                    while j < len(ss) and ss[j][1] < s[1] + horizon * 1000:
                        j += 1
                    if j >= len(ss):
                        break
                    if s[6] > k:
                        pnl += (ss[j][2] - s[4]) - fee(s[4]); n += 1
                    elif s[6] < -k:
                        pnl += (s[3] - ss[j][2]) - fee(s[3]); n += 1
            print(f"   |I3| > {k}: {n} trades, avg PnL {pnl / n if n else 0:+.4f}/contract (exit at mid; fee + half-spread paid)")


if __name__ == "__main__":
    main()
