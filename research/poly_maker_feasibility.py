"""Would the favourite-maker work on Polymarket? (read-only, no wallet needed)

The Kalshi version rests bids on favourites priced 74-93c near resolution and earns
the spread it never pays, because makers are free. Polymarket also charges takers
only, so the same shape could exist there — but only if the books actually leave a
spread to capture at the top of the book.

For the most liquid markets resolving soon, this measures:
  * how many sit in the favourite band
  * the bid-ask spread there (the gross maker capture)
  * depth at the touch (how much sits ahead of a new order in the queue)
"""
import json
import statistics
import sys
import time
import urllib.request
from datetime import datetime, timezone

G = "https://gamma-api.polymarket.com"
C = "https://clob.polymarket.com"


def get(u, tries=4):
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "marketsbot-research"}), timeout=25) as r:
                d = json.load(r)
            time.sleep(0.2)
            return d
        except Exception:  # noqa: BLE001
            time.sleep(1.5 * (i + 1))
    return None


def arg(n, d):
    return type(d)(sys.argv[sys.argv.index(n) + 1]) if n in sys.argv else d


def main():
    limit = arg("--limit", 300)
    lo, hi = arg("--lo", 0.74), arg("--hi", 0.93)
    ms = []
    for off in range(0, limit, 100):
        page = get(f"{G}/markets?limit=100&offset={off}&active=true&closed=false&order=volume24hr&ascending=false")
        if not page:
            break
        ms.extend(page)
    print(f"{len(ms)} active markets pulled")
    now = datetime.now(timezone.utc)
    rows = []
    for m in ms:
        try:
            bid, ask = m.get("bestBid"), m.get("bestAsk")
            if bid is None or ask is None:
                continue
            bid, ask = float(bid), float(ask)
            mid = (bid + ask) / 2
            if not (lo <= mid <= hi):
                continue
            end = m.get("endDate")
            days = (datetime.fromisoformat(end.replace("Z", "+00:00")) - now).total_seconds() / 86400 if end else 999
            if days > 3:
                continue
            toks = json.loads(m.get("clobTokenIds", "[]") or "[]")
            if not toks:
                continue
            book = get(f"{C}/book?token_id={toks[0]}")
            if not book:
                continue
            bids = sorted(((float(b["price"]), float(b["size"])) for b in book.get("bids", [])), reverse=True)
            asks = sorted((float(a["price"]), float(a["size"])) for a in book.get("asks", []))
            if not bids or not asks:
                continue
            spread = asks[0][0] - bids[0][0]
            fs = m.get("feeSchedule") or {}
            rows.append({"q": m.get("question", "")[:52], "mid": mid, "spread": spread, "top_bid_size": bids[0][1],
                         "top_ask_size": asks[0][1], "days": days, "vol24": m.get("volume24hr") or 0,
                         "fee_rate": fs.get("rate"), "taker_only": fs.get("takerOnly"), "tick": m.get("orderPriceMinTickSize")})
        except Exception:  # noqa: BLE001
            continue
    if not rows:
        print("no markets in the favourite band resolving within 3 days")
        return
    rows.sort(key=lambda r: -r["vol24"])
    print(f"\n{len(rows)} favourites ({lo}-{hi}) resolving within 3 days\n")
    print(f"{'market':<52} {'mid':>6} {'spread':>7} {'bid depth':>10} {'days':>6} {'fee':>6} {'24h vol':>12}")
    for r in rows[:20]:
        print(f"{r['q']:<52} {r['mid']:>6.3f} {r['spread']:>7.3f} {r['top_bid_size']:>10.0f} {r['days']:>6.2f} "
              f"{str(r['fee_rate']):>6} {r['vol24']:>12,.0f}")
    sp = [r["spread"] for r in rows]
    dp = [r["top_bid_size"] for r in rows]
    print(f"\nmedian spread {statistics.median(sp):.3f}  (this is the gross maker capture per contract)")
    print(f"median depth at the best bid {statistics.median(dp):,.0f} contracts  (this much sits ahead of a new resting order)")
    print(f"markets with taker-only fees: {sum(1 for r in rows if r['taker_only'])}/{len(rows)} (makers free)")
    tiny = [r for r in rows if r["top_bid_size"] < 500]
    print(f"markets where fewer than 500 contracts sit ahead: {len(tiny)} — these are where a small order could reach the front")


if __name__ == "__main__":
    main()
