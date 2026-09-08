"""Measure the information lag between the OFFICIAL MLB feed and Kalshi's live markets.

Kalshi's game markets (winner, total runs) stay open for days after first pitch, so
they trade throughout the game. MLB's own Stats API — the feed that powers Gameday and
the same data the markets settle on — publishes each completed play within ~1 second.
Most participants instead watch a broadcast or stream that runs 20–45 s behind.

If Kalshi's quotes only move well after a run appears on the official feed, that gap is
a repeatable edge on the highest-volume non-crypto markets on the exchange.

This records both sides at ~2 s resolution and reports, for every scoring play:
  * seconds from the play's official endTime until Kalshi's mid moved >= 2 cents
  * how far the mid moved, and what was quoted in between

Usage: research/venv/bin/python research/mlb_latency.py [--hours 5] [--poll 2]
Writes reports/mlb-latency-<ts>.jsonl and prints a summary at the end.
"""
import json
import re
import sys
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

K = "https://external-api.kalshi.com/trade-api/v2"
MLB = "https://statsapi.mlb.com/api/v1"
MLB11 = "https://statsapi.mlb.com/api/v1.1"


def arg(n, d):
    return type(d)(sys.argv[sys.argv.index(n) + 1]) if n in sys.argv else d


def get(url, tries=3, timeout=12):
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "marketsbot-research"}), timeout=timeout) as r:
                return json.load(r)
        except Exception:  # noqa: BLE001
            time.sleep(0.6 * (i + 1))
    return {}


def kalshi_game_markets():
    """Open game markets keyed by the ticker's game code (e.g. 26SEP082140TEXSEA)."""
    out = defaultdict(list)
    for series in ("KXMLBGAME", "KXMLBTOTAL"):
        d = get(f"{K}/markets?series_ticker={series}&status=open&limit=500&mve_filter=exclude")
        for m in d.get("markets", []):
            parts = m["ticker"].split("-")
            if len(parts) >= 2:
                out[parts[1]].append(m)
        time.sleep(0.4)
    return out


def parse_code(code):
    """26SEP082140TEXSEA -> (date, hhmm, away, home) using team abbreviations."""
    m = re.match(r"^(\d{2}[A-Z]{3}\d{2})(\d{4})([A-Z]+)$", code)
    if not m:
        return None
    teams = m.group(3)
    return m.group(1), m.group(2), teams


def main():
    hours = arg("--hours", 5.0)
    poll = arg("--poll", 2.0)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    sched = get(f"{MLB}/schedule?sportId=1&date={today}")
    games = [g for d in sched.get("dates", []) for g in d.get("games", [])]
    print(f"{len(games)} MLB games on {today}")
    kal = kalshi_game_markets()
    print(f"{len(kal)} Kalshi game codes open")

    # team id -> abbreviation (the schedule endpoint omits abbreviations)
    tm = {t["id"]: t.get("abbreviation", "") for t in get(f"{MLB}/teams?sportId=1").get("teams", [])}
    # Kalshi uses its own short codes for a few clubs
    ALIAS = {"ARI": ["AZ", "ARI"], "CHW": ["CWS", "CHW"], "TB": ["TBA", "TB"], "WSH": ["WSH", "WAS"],
             "SD": ["SD", "SDP"], "SF": ["SF", "SFG"], "KC": ["KC", "KCR"], "ATH": ["ATH", "OAK"], "LAD": ["LAD"], "NYY": ["NYY"]}

    def variants(ab):
        return ALIAS.get(ab, [ab])

    matched = []
    for g in games:
        away = tm.get(g["teams"]["away"]["team"]["id"], "")
        home = tm.get(g["teams"]["home"]["team"]["id"], "")
        if not away or not home:
            continue
        for code, ms in kal.items():
            p = parse_code(code)
            if not p:
                continue
            teams = p[2]
            if any(teams == a + h for a in variants(away) for h in variants(home)):
                matched.append((g["gamePk"], f"{away}@{home}", code, ms))
                break
    print(f"matched {len(matched)} games to Kalshi markets:")
    for pk, name, code, ms in matched:
        print(f"   {name:<10} game {pk} -> {code} ({len(ms)} markets, e.g. {ms[0]['ticker']})")
    if not matched:
        print("no matches; exiting")
        return

    out = open(f"reports/mlb-latency-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M')}.jsonl", "w")
    t_end = time.time() + hours * 3600
    last_play = {}      # gamePk -> last play index seen
    events = []         # scoring plays awaiting a Kalshi move
    quotes_hist = defaultdict(list)  # ticker -> [(ts, mid)]
    resolved = []

    while time.time() < t_end:
        loop_start = time.time()
        for pk, name, code, ms in matched:
            live = get(f"{MLB11}/game/{pk}/feed/live", tries=1, timeout=8)
            if not live:
                continue
            ld = live.get("liveData", {})
            plays = ld.get("plays", {}).get("allPlays", [])
            ls = ld.get("linescore", {})
            state = live.get("gameData", {}).get("status", {}).get("abstractGameState")
            now = time.time()
            # Kalshi quotes for this game
            qs = {}
            for m in ms:
                d = get(f"{K}/markets/{m['ticker']}", tries=1, timeout=8)
                mk = d.get("market", {})
                b, a = mk.get("yes_bid_dollars"), mk.get("yes_ask_dollars")
                if b and a:
                    mid = (float(b) + float(a)) / 2
                    qs[m["ticker"]] = mid
                    quotes_hist[m["ticker"]].append((now, mid))
            out.write(json.dumps({"ts": now, "game": name, "state": state, "inning": ls.get("currentInning"),
                                  "half": ls.get("inningHalf"), "away": ls.get("teams", {}).get("away", {}).get("runs"),
                                  "home": ls.get("teams", {}).get("home", {}).get("runs"), "quotes": qs}) + "\n")
            out.flush()
            # detect newly completed scoring plays
            n = len(plays)
            prev = last_play.get(pk, n if state != "Live" else 0)
            for p in plays[prev:]:
                if not p.get("about", {}).get("isComplete"):
                    continue
                if p.get("about", {}).get("isScoringPlay"):
                    et = p["about"].get("endTime")
                    ets = datetime.fromisoformat(et.replace("Z", "+00:00")).timestamp() if et else now
                    ev = {"game": name, "pk": pk, "event": p["result"].get("event"), "desc": (p["result"].get("description") or "")[:70],
                          "inning": p["about"].get("inning"), "half": p["about"].get("halfInning"),
                          "official_ts": ets, "seen_ts": now, "score": f"{p['result'].get('awayScore')}-{p['result'].get('homeScore')}",
                          "mids_at_detect": dict(qs)}
                    events.append(ev)
                    print(f"[{datetime.fromtimestamp(now, timezone.utc).strftime('%H:%M:%S')}] SCORE {name} inn {ev['inning']} {ev['event']} -> {ev['score']} "
                          f"(official {ets and datetime.fromtimestamp(ets, timezone.utc).strftime('%H:%M:%S')}, we saw it {now-ets:.1f}s later)")
                    out.write(json.dumps({"scoring_play": ev}) + "\n"); out.flush()
            last_play[pk] = n
        # resolve pending events: has any market moved >= 2c since the play?
        for ev in list(events):
            if time.time() - ev["official_ts"] > 300:
                events.remove(ev); resolved.append((ev, None, None)); continue
            for tk, m0 in ev["mids_at_detect"].items():
                for ts, mid in quotes_hist[tk]:
                    if ts > ev["seen_ts"] and abs(mid - m0) >= 0.02:
                        lag = ts - ev["official_ts"]
                        resolved.append((ev, tk, lag))
                        print(f"     -> {tk} moved {m0:.2f}->{mid:.2f}, {lag:.1f}s after the official play")
                        events.remove(ev)
                        break
                else:
                    continue
                break
        time.sleep(max(0.0, poll - (time.time() - loop_start)))

    out.close()
    lags = [l for _, _, l in resolved if l is not None]
    print(f"\n{len(resolved)} scoring plays tracked, {len(lags)} produced a >=2c quote move")
    if lags:
        lags.sort()
        print(f"  median lag official-play -> Kalshi move: {lags[len(lags)//2]:.1f}s  (min {lags[0]:.1f}s, max {lags[-1]:.1f}s)")
        print("  A median well above a few seconds means the market is trading on delayed information.")


if __name__ == "__main__":
    main()
