"""Are Kalshi's combo (multivariate) contracts overpriced, the way parlays always are?

A parlay is the most reliably mispriced product in betting: books quote the combo
above the product of its legs and punters take it anyway. If Kalshi does the same,
selling the combo is the trade.

NOTE ON A PAST ERROR: an earlier version of this scan read `yes_bid`/`volume` and
concluded no combo market had any liquidity. Kalshi names those fields
`yes_bid_dollars` and `volume_fp`; the scan was reading absent keys and reporting
absence of data as absence of liquidity. Always confirm a field exists before
concluding something is zero.
"""
import sys, time
sys.path.insert(0, "research")
from kalshi_auth import get, markets  # noqa: E402


def f(m, k, d=0.0):
    v = m.get(k)
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def main():
    live, scanned = [], 0
    for m in markets(status="open", mve_filter="only"):
        scanned += 1
        vol, oi = f(m, "volume_fp"), f(m, "open_interest_fp")
        bid, ask = f(m, "yes_bid_dollars"), f(m, "yes_ask_dollars")
        if vol > 0 or oi > 0 or bid > 0:
            live.append({"ticker": m["ticker"], "title": m.get("title", ""), "bid": bid, "ask": ask,
                         "vol": vol, "oi": oi, "legs": m.get("yes_sub_title", "")})
        if scanned >= 30000:
            break
    print(f"scanned {scanned} open combo markets; {len(live)} carry volume, open interest or a bid\n")
    if not live:
        print("no combo market is quoted -- nothing to trade here")
        return
    live.sort(key=lambda r: -(r["vol"] + r["oi"]))
    print(f"{'bid':>6}{'ask':>6}{'volume':>10}{'open int':>10}  legs")
    for r in live[:25]:
        print(f"{r['bid']:>6.2f}{r['ask']:>6.2f}{r['vol']:>10,.0f}{r['oi']:>10,.0f}  {r['legs'][:70]}")
    print(f"\ntotal combo volume: {sum(r['vol'] for r in live):,.0f} contracts")
    print(f"combos with a real two-sided quote: {sum(1 for r in live if r['bid'] > 0 and r['ask'] > 0)}")


if __name__ == "__main__":
    main()
