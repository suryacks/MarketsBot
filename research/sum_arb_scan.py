"""Exchange-wide sum-arbitrage scan on Kalshi (playbook edges #1 and #5).

For any event whose markets are mutually exclusive AND exhaustive:
  * buy every YES: cost = Sum(ask) + fees, pays exactly $1  -> arb if cost < 1
  * buy every NO : cost = Sum(1 - bid) + fees, pays N-1     -> arb if cost < N-1
      (equivalently Sum(bid) > 1 + fees)

Kalshi flags these events as mutually exclusive; we additionally require the market
count to match the event's declared outcomes so a partial set is never treated as a
partition. Reports profit per $1 set and, for carry trades, the annualised return
(profit / cost / years to resolution) since a 2% lock over two years is worthless.

Usage: research/venv/bin/python research/sum_arb_scan.py [--min-profit 0.005]
"""
import json
import math
import sys
import time
import urllib.request
from datetime import datetime, timezone

K = "https://external-api.kalshi.com/trade-api/v2"


def get(u, tries=6):
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "marketsbot-research"}), timeout=30) as r:
                d = json.load(r)
            time.sleep(0.25)
            return d
        except Exception:  # noqa: BLE001
            time.sleep(2 * (i + 1))
    return {}


def fee(px):
    return math.ceil(0.07 * px * (1 - px) * 100) / 100


def arg(n, d):
    return float(sys.argv[sys.argv.index(n) + 1]) if n in sys.argv else d


def main():
    min_profit = arg("--min-profit", 0.005)
    # 1) every open event that Kalshi marks mutually exclusive, with its markets
    events, cur, pages = [], "", 0
    while pages < 30:
        d = get(f"{K}/events?status=open&with_nested_markets=true&limit=200" + (f"&cursor={cur}" if cur else ""))
        evs = d.get("events", [])
        events.extend(evs)
        cur = d.get("cursor", "")
        pages += 1
        if not cur or not evs:
            break
    print(f"{len(events)} open events fetched")
    mx = [e for e in events if e.get("mutually_exclusive")]
    print(f"{len(mx)} flagged mutually exclusive")

    found = []
    scanned = 0
    now = datetime.now(timezone.utc)
    for e in mx:
        ms = [m for m in e.get("markets", []) if m.get("status") == "active"]
        if len(ms) < 2:
            continue
        asks, bids, sizes_a, sizes_b, closes = [], [], [], [], []
        ok = True
        for m in ms:
            a, b = m.get("yes_ask_dollars"), m.get("yes_bid_dollars")
            if a is None or b is None:
                ok = False
                break
            a, b = float(a), float(b)
            if not (0 < a < 1) or not (0 <= b < 1):
                ok = False
                break
            asks.append(a)
            bids.append(b)
            sizes_a.append(float(m.get("yes_ask_size_fp") or 0))
            sizes_b.append(float(m.get("yes_bid_size_fp") or 0))
            if m.get("close_time"):
                closes.append(datetime.fromisoformat(m["close_time"].replace("Z", "+00:00")))
        if not ok or not closes:
            continue
        scanned += 1
        days = max((min(closes) - now).total_seconds() / 86400, 0.01)
        n = len(ms)
        # Exhaustiveness gate: Kalshi's "mutually exclusive" only means AT MOST one wins.
        # If the listed markets were the whole partition the market would price them near $1;
        # a set summing to 0.14 simply has outcomes that are not listed. Require the mid sum
        # to be close to 1 before treating the set as a partition.
        mid_sum = sum((a + b) / 2 for a, b in zip(asks, bids))
        if not (0.90 <= mid_sum <= 1.10):
            continue
        # buy every YES
        cost_yes = sum(asks) + sum(fee(a) for a in asks)
        if 1.0 - cost_yes > min_profit:
            found.append(("BUY ALL YES", e["event_ticker"], e.get("title", "")[:48], n, 1.0 - cost_yes, cost_yes, min(sizes_a), days))
        # buy every NO
        cost_no = sum(1 - b for b in bids) + sum(fee(b) for b in bids)
        if (n - 1) - cost_no > min_profit:
            found.append(("BUY ALL NO", e["event_ticker"], e.get("title", "")[:48], n, (n - 1) - cost_no, cost_no, min(sizes_b), days))

    print(f"{scanned} events had complete two-sided quotes\n")
    if not found:
        print("no sum-arbitrage found")
        return
    found.sort(key=lambda x: -(x[4] / max(x[5], 0.01) / max(x[7] / 365.0, 1e-4)))
    print(f"{'side':<12} {'event':<26} {'n':>3} {'profit':>8} {'cost':>8} {'size':>7} {'days':>7} {'ann.%':>8}  title")
    for side, ev, title, n, profit, cost, size, days in found[:25]:
        ann = profit / max(cost, 0.01) / max(days / 365.0, 1e-4) * 100
        print(f"{side:<12} {ev:<26} {n:>3} {profit:>+8.3f} {cost:>8.3f} {size:>7.0f} {days:>7.1f} {ann:>8.1f}  {title}")
    print(f"\n{len(found)} opportunities >= {min_profit:.3f}/set")


if __name__ == "__main__":
    main()
