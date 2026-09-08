"""Live sports information lag: ESPN scoreboard (stadium feed) vs Kalshi's
in-game market. Polls both every few seconds for one game and logs
score/clock changes alongside the Kalshi YES bid/ask, so the repricing lag
after each score change can be measured afterwards.

Usage: python research/ingame_lag.py --series KXNFLGAME --hours 4
Writes reports/ingame-<date>.jsonl (one line per poll) and prints a summary
of score changes and the seconds until the Kalshi mid moved ≥ 3¢.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone

K = "https://external-api.kalshi.com/trade-api/v2"
ESPN = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"


def get(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "marketsbot-research"}), timeout=15) as r:
        return json.load(r)


def arg(name, default):
    return sys.argv[sys.argv.index(name) + 1] if name in sys.argv else default


def main():
    series = arg("--series", "KXNFLGAME")
    hours = float(arg("--hours", 4))
    poll = float(arg("--poll", 4))
    # find live/open game markets in the series
    ms = get(f"{K}/markets?series_ticker={series}&status=open&limit=500&mve_filter=exclude").get("markets", [])
    horizon = time.time() + 8 * 3600
    ms = [m for m in ms if float(m.get("volume_fp") or 0) > 0 and datetime.fromisoformat(m["close_time"].replace("Z", "+00:00")).timestamp() < horizon]
    ms.sort(key=lambda m: (m["close_time"], -float(m.get("volume_fp") or 0)))
    if not ms:
        print("no markets in", series, "closing within 8 h"); return
    watch = ms[:4]
    print("watching:", [(m["ticker"], m["title"][:50]) for m in watch])
    out = open(f"reports/ingame-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M')}.jsonl", "w")
    t_end = time.time() + hours * 3600
    last_scores = {}
    changes = []
    last_mid = {}
    while time.time() < t_end:
        ts = time.time()
        try:
            sb = get(ESPN)
            games = {}
            for e in sb.get("events", []):
                comp = e["competitions"][0]
                teams = {c["homeAway"]: (c["team"]["abbreviation"], int(c.get("score") or 0)) for c in comp["competitors"]}
                games[e["id"]] = {"name": e.get("shortName"), "state": e["status"]["type"]["state"], "clock": e["status"].get("displayClock"), "period": e["status"].get("period"), "home": teams.get("home"), "away": teams.get("away")}
        except Exception as ex:  # noqa: BLE001
            games = {"error": str(ex)}
        quotes = {}
        for m in watch:
            try:
                q = get(f"{K}/markets/{m['ticker']}")["market"]
                quotes[m["ticker"]] = {"bid": q.get("yes_bid_dollars"), "ask": q.get("yes_ask_dollars"), "last": q.get("last_price_dollars")}
            except Exception as ex:  # noqa: BLE001
                quotes[m["ticker"]] = {"error": str(ex)}
        rec = {"ts": ts, "games": games, "quotes": quotes}
        out.write(json.dumps(rec) + "\n"); out.flush()
        # detect score changes
        for gid, g in games.items():
            if not isinstance(g, dict) or g.get("state") != "in":
                continue
            sc = (g["home"][1], g["away"][1]) if g.get("home") and g.get("away") else None
            if sc and last_scores.get(gid) not in (None, sc):
                changes.append((ts, g["name"], last_scores[gid], sc))
                print(f"{datetime.fromtimestamp(ts, timezone.utc).strftime('%H:%M:%S')} SCORE {g['name']} {last_scores[gid]} -> {sc}  quotes: {quotes}")
            last_scores[gid] = sc
        for tk, q in quotes.items():
            try:
                mid = (float(q["bid"]) + float(q["ask"])) / 2
                if tk in last_mid and abs(mid - last_mid[tk]) >= 0.03:
                    print(f"{datetime.fromtimestamp(ts, timezone.utc).strftime('%H:%M:%S')} KALSHI {tk} mid {last_mid[tk]:.2f} -> {mid:.2f}")
                last_mid[tk] = mid
            except (TypeError, ValueError, KeyError):
                pass
        time.sleep(max(0.0, poll - (time.time() - ts)))
    out.close()
    print(f"done: {len(changes)} score changes logged")


if __name__ == "__main__":
    main()
