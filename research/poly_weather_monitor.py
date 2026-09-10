"""Does Polymarket leave a price on weather buckets its own data has already killed?

Polymarket settles temperature on the NOAA hourly observations themselves -- "the highest
reading under the Temp column for all times on this day" -- not on the official climate
report Kalshi uses. That removes the one residual risk in the Kalshi lock: there is no
sampling gap, because the readings we watch ARE the settlement. A bucket the hourly maximum
has already passed cannot win, with certainty rather than high probability.

Whether that is worth money is a separate question, and the answer on Kalshi was no: decided
buckets are repriced to a cent before they can be traded. This records, minute by minute,
the moment each bucket dies and what price was still standing, so the question is settled
with evidence instead of hope. Read-only; needs no wallet.

Usage: research/venv/bin/python research/poly_weather_monitor.py [--hours 24]
"""
import io, json, re, sys, time, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

G = "https://gamma-api.polymarket.com"
# city -> (IEM network, station, UTC offset). Stations are taken from the resolution text.
STN = {
    "Chicago": ("IL_ASOS", "ORD", -5), "New York City": ("NY_ASOS", "NYC", -4),
    "Miami": ("FL_ASOS", "MIA", -4), "Los Angeles": ("CA_ASOS", "LAX", -7),
    "Austin": ("TX_ASOS", "AUS", -5), "Denver": ("CO_ASOS", "DEN", -6),
    "Atlanta": ("GA_ASOS", "ATL", -4), "Houston": ("TX_ASOS", "IAH", -5),
    "Dallas": ("TX_ASOS", "DFW", -5), "Seattle": ("WA_ASOS", "SEA", -7),
    "San Francisco": ("CA_ASOS", "SFO", -7),
}


def arg(n, d):
    return type(d)(sys.argv[sys.argv.index(n) + 1]) if n in sys.argv else d


def get(u, tries=3):
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "marketsbot-research"}), timeout=30) as r:
                return json.load(r)
        except Exception:  # noqa: BLE001
            time.sleep(2 * (i + 1))
    return []


def day_max(net, stn, day, off):
    """Highest hourly reading for the local day, in F -- the number Polymarket settles on."""
    q = urllib.parse.urlencode({
        "station": stn, "network": net, "data": "tmpf", "year1": day.year, "month1": day.month, "day1": day.day,
        "year2": (day + timedelta(days=2)).year, "month2": (day + timedelta(days=2)).month, "day2": (day + timedelta(days=2)).day,
        "tz": "Etc/UTC", "format": "onlycomma", "latlon": "no", "missing": "M", "trace": "T", "direct": "no",
    }) + "&report_type=2&report_type=3"
    try:
        with urllib.request.urlopen("https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?" + q, timeout=200) as r:
            text = r.read().decode()
    except Exception:  # noqa: BLE001
        return None
    vals = []
    for line in io.StringIO(text):
        p = line.strip().split(",")
        if len(p) < 3 or p[0] == "station":
            continue
        try:
            t = datetime.strptime(p[1][:16], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            v = float(p[2])
        except ValueError:
            continue
        if (t + timedelta(hours=off)).date() == day.date():
            vals.append(v)
    return max(vals) if vals else None


def open_markets():
    now = datetime.now(timezone.utc)
    out = []
    for off in range(0, 1500, 100):
        ms = get(f"{G}/markets?limit=100&offset={off}&active=true&closed=false&order=volume24hr&ascending=false")
        if not ms:
            break
        for m in ms:
            q = m.get("question") or ""
            if "temperature" not in q.lower() or "°F" not in q or not m.get("endDate"):
                continue
            if (datetime.fromisoformat(m["endDate"].replace("Z", "+00:00")) - now).total_seconds() <= 0:
                continue
            rng = re.search(r"(\d+)-(\d+)°F", q)
            city = q.split(" in ")[-1].split(" be ")[0]
            if not rng or city not in STN:
                continue
            out.append({"id": m.get("conditionId"), "q": q, "city": city, "lo": int(rng.group(1)), "hi": int(rng.group(2)),
                        "end": m["endDate"], "bid": m.get("bestBid"), "ask": m.get("bestAsk"),
                        "vol24": float(m.get("volume24hr") or 0)})
    return out


def main():
    hours = arg("--hours", 24)
    deadline = time.time() + hours * 3600
    seen_dead = {}
    events = []
    print(f"watching Polymarket weather for {hours}h; recording the price standing when each bucket dies", flush=True)
    while time.time() < deadline:
        mkts = open_markets()
        cache = {}
        for m in mkts:
            net, stn, off = STN[m["city"]]
            day = datetime.fromisoformat(m["end"].replace("Z", "+00:00")) + timedelta(hours=off)
            key = (stn, day.date())
            if key not in cache:
                cache[key] = day_max(net, stn, datetime(day.year, day.month, day.day, tzinfo=timezone.utc), off)
                time.sleep(1)
            mx = cache[key]
            if mx is None or mx <= m["hi"]:
                continue
            # this bucket can no longer contain the day's maximum
            if m["q"] in seen_dead:
                continue
            seen_dead[m["q"]] = True
            ev = {"ts": datetime.now(timezone.utc).isoformat(), "q": m["q"], "city": m["city"],
                  "bucket": [m["lo"], m["hi"]], "observed_max": mx, "bid_when_dead": m["bid"], "ask_when_dead": m["ask"],
                  "vol24": m["vol24"]}
            events.append(ev)
            print(f"  DEAD {m['city']:<14} [{m['lo']}-{m['hi']}] observed {mx:.0f}F  bid {m['bid']}  (sellable if bid is real)", flush=True)
        json.dump({"kind": "poly-weather", "events": events}, open("reports/poly-weather.json", "w"), indent=1)
        time.sleep(300)
    print(f"\n{len(events)} buckets died while watched")
    sellable = [e for e in events if (e["bid_when_dead"] or 0) >= 0.03]
    print(f"{len(sellable)} still had a bid of 3c or better -- those are the tradeable ones")


if __name__ == "__main__":
    main()
