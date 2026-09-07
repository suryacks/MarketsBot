"""Feasibility study: can a public forecast beat Kalshi's daily high-temperature markets?

For every settled market in a series (e.g. KXHIGHNY):
  * fair = P(actual max temp lands in the bucket) under actual ~ Normal(forecast + bias, sigma),
    with bias/sigma estimated empirically from Open-Meteo archived forecasts vs. ACIS actuals
    over a disjoint calibration window (no look-ahead: parameters fit on dates before the test set);
  * market = Kalshi YES price at a fixed decision time (hourly candlesticks), e.g. 00:00 UTC of the
    market day (8 pm ET the evening before);
  * score Brier(model) vs Brier(market), and a simple rule: buy YES if fair - ask > edge, buy NO if
    bid - fair > edge, hold to settlement, Kalshi 7 % quadratic taker fee.

Usage: python research/weather_feasibility.py [SERIES] [--decision-hour 0] [--edge 0.05]
Requires only the standard library. Kalshi public endpoints are rate limited: paced at ~3 req/s.
"""
import json
import math
import statistics
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

K = "https://external-api.kalshi.com/trade-api/v2"
OM_HIST = "https://historical-forecast-api.open-meteo.com/v1/forecast"
ACIS = "https://data.rcc-acis.org/StnData"

# series -> (station id for ACIS, lat, lon, IANA tz)
STATIONS = {
    "KXHIGHNY": ("KNYC", 40.7789, -73.9692, "America/New_York"),
    "KXHIGHCHI": ("KMDW", 41.7868, -87.7522, "America/Chicago"),
    "KXHIGHMIA": ("KMIA", 25.7959, -80.2870, "America/New_York"),
    "KXHIGHLAX": ("KLAX", 33.9382, -118.3866, "America/Los_Angeles"),
    "KXHIGHAUS": ("KAUS", 30.1945, -97.6699, "America/Chicago"),
    "KXHIGHPHIL": ("KPHL", 39.8729, -75.2437, "America/New_York"),
    "KXHIGHDEN": ("KDEN", 39.8466, -104.6562, "America/Denver"),
}


def get(url, tries=6, pause=0.35):
    for i in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                data = json.load(r)
            time.sleep(pause)
            return data
        except Exception as e:  # noqa: BLE001
            time.sleep(1.5 * (i + 1))
            last = e
    raise last


def post_json(url, body):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def settled_markets(series):
    out, cur = [], ""
    while True:
        d = get(f"{K}/markets?series_ticker={series}&status=settled&limit=1000&mve_filter=exclude" + (f"&cursor={cur}" if cur else ""))
        ms = d.get("markets", [])
        out.extend(ms)
        cur = d.get("cursor", "")
        if not cur or not ms:
            break
    return out


def market_day(m):
    """Kalshi closes these at ~05:00Z (or later for western cities) the day after; the market day is the local date of (close - 6h)."""
    close = datetime.fromisoformat(m["close_time"].replace("Z", "+00:00"))
    return (close - timedelta(hours=6)).date()


def bucket(m):
    """Return (lo, hi) of integer temps that resolve YES (inclusive), or None if unparsable."""
    st = m.get("strike_type")
    fl, cap = m.get("floor_strike"), m.get("cap_strike")
    if st == "between" and fl is not None and cap is not None:
        return (math.ceil(fl), math.floor(cap))
    if st in ("greater", "greater_or_equal") and fl is not None:
        return (math.floor(fl) + (1 if st == "greater" else 0), 999)
    if st in ("less", "less_or_equal") and fl is not None:
        return (-999, math.ceil(fl) - (1 if st == "less" else 0))
    if st == "custom":
        return None
    # tickers like -B82.5 (between 82 and 83), -T87 (>87 ... or <=?) — fall back to ticker parsing
    tick = m["ticker"].split("-")[-1]
    if tick.startswith("B"):
        c = float(tick[1:])
        return (math.floor(c), math.ceil(c))
    return None


def forecasts(lat, lon, tz, start, end):
    """Open-Meteo archived forecast of daily max (short-lead). Returns {date: forecast_F}."""
    q = urllib.parse.urlencode({
        "latitude": lat, "longitude": lon, "start_date": start, "end_date": end,
        "daily": "temperature_2m_max", "temperature_unit": "fahrenheit", "timezone": tz,
    })
    d = get(f"{OM_HIST}?{q}", pause=0.5)
    return {t: v for t, v in zip(d["daily"]["time"], d["daily"]["temperature_2m_max"]) if v is not None}


OM_PREV = "https://previous-runs-api.open-meteo.com/v1/forecast"


def forecasts_prev(lat, lon, tz, lead_days=1, past_days=92):
    """Daily max implied by the forecast issued `lead_days` before (Open-Meteo previous-runs API,
    hourly `temperature_2m_previous_dayN` aggregated to a local-date max). Last ~92 days only."""
    var = f"temperature_2m_previous_day{lead_days}"
    q = urllib.parse.urlencode({
        "latitude": lat, "longitude": lon, "hourly": var, "temperature_unit": "fahrenheit",
        "timezone": tz, "past_days": past_days, "forecast_days": 1,
    })
    d = get(f"{OM_PREV}?{q}", pause=0.5)
    out = {}
    for t, v in zip(d["hourly"]["time"], d["hourly"][var]):
        if v is None:
            continue
        day = t[:10]
        out[day] = max(out.get(day, -999.0), v)
    return out


def actuals(sid, start, end):
    d = post_json(ACIS, {"sid": sid, "sdate": start, "edate": end, "elems": [{"name": "maxt"}]})
    return {t: float(v) for t, v in d["data"] if v not in ("M", "T", "")}


def market_price_at(series, m, when_utc):
    """YES mid/close from hourly candlesticks in the hour ending at or before `when_utc`."""
    start = int(when_utc.timestamp()) - 6 * 3600
    end = int(when_utc.timestamp())
    d = get(f"{K}/series/{series}/markets/{m['ticker']}/candlesticks?start_ts={start}&end_ts={end}&period_interval=60")
    cs = d.get("candlesticks", [])
    if not cs:
        return None
    c = cs[-1]
    bid = c.get("yes_bid", {}).get("close_dollars")
    ask = c.get("yes_ask", {}).get("close_dollars")
    if bid and ask and float(bid) > 0 and float(ask) < 1:
        return float(bid), float(ask)
    p = c.get("price", {}).get("close_dollars")
    return (float(p), float(p)) if p else None


def norm_cdf(x):
    return 0.5 * math.erfc(-x / math.sqrt(2))


def fair_prob(lo, hi, mu, sigma):
    """P(round(X) in [lo, hi]) for X ~ N(mu, sigma) — actuals are whole degrees."""
    a = -1e9 if lo <= -999 else lo - 0.5
    b = 1e9 if hi >= 999 else hi + 0.5
    return norm_cdf((b - mu) / sigma) - norm_cdf((a - mu) / sigma)


def kalshi_fee(px, n=1.0):
    return math.ceil(0.07 * n * px * (1 - px) * 100) / 100


def main():
    series = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else "KXHIGHNY"
    decision_hour = int(sys.argv[sys.argv.index("--decision-hour") + 1]) if "--decision-hour" in sys.argv else 0
    edge = float(sys.argv[sys.argv.index("--edge") + 1]) if "--edge" in sys.argv else 0.05
    # --lead 0: archived same-day forecast (optimistic upper bound); --lead 1: forecast issued the day before
    lead = int(sys.argv[sys.argv.index("--lead") + 1]) if "--lead" in sys.argv else 0
    sid, lat, lon, tz = STATIONS[series]

    ms = [m for m in settled_markets(series) if m.get("result") in ("yes", "no")]
    days = sorted({market_day(m) for m in ms})
    print(f"{series}: {len(ms)} settled markets over {len(days)} days ({days[0]} .. {days[-1]})")

    ac = actuals(sid, (days[0] - timedelta(days=130)).isoformat(), days[-1].isoformat())
    if lead == 0:
        # forecast error model: fit on the 120 days BEFORE the first market day (no look-ahead)
        fit_end = days[0] - timedelta(days=1)
        fit_start = fit_end - timedelta(days=120)
        fc_fit = forecasts(lat, lon, tz, fit_start.isoformat(), fit_end.isoformat())
        errs = [ac[d] - fc_fit[d] for d in fc_fit if d in ac]
        fc = forecasts(lat, lon, tz, days[0].isoformat(), days[-1].isoformat())
        print(f"lead 0 (same-day archived forecast — optimistic)")
    else:
        # previous-runs archive only reaches back ~92 days: fit the error model on the first
        # third of the available window and test on the rest (walk-forward would be better)
        fc_all = forecasts_prev(lat, lon, tz, lead_days=lead)
        keys = sorted(k for k in fc_all if k in ac)
        cut = keys[len(keys) // 3]
        errs = [ac[d] - fc_all[d] for d in keys if d < cut]
        fc = {d: v for d, v in fc_all.items() if d >= cut}
        days = [d for d in days if d.isoformat() >= cut]
        print(f"lead {lead} day(s): error model fit on {len(errs)} days before {cut}, test on {len(days)} market days after")
    bias, sigma = statistics.mean(errs), statistics.pstdev(errs)
    print(f"forecast error (actual - forecast) on {len(errs)} calibration days: bias {bias:+.2f}F sigma {sigma:.2f}F")

    rows = []
    for m in ms:
        d = market_day(m)
        b = bucket(m)
        if b is None or d.isoformat() not in fc:
            continue
        when = datetime(d.year, d.month, d.day, decision_hour, tzinfo=timezone.utc)
        px = market_price_at(series, m, when)
        if px is None:
            continue
        mu = fc[d.isoformat()] + bias
        fair = fair_prob(b[0], b[1], mu, sigma)
        y = 1.0 if m["result"] == "yes" else 0.0
        rows.append((d, m["ticker"], b, fc[d.isoformat()], ac.get(d.isoformat()), px[0], px[1], fair, y))

    n = len(rows)
    if n == 0:
        print("no rows")
        return
    mid = lambda r: (r[5] + r[6]) / 2
    bm = sum((r[7] - r[8]) ** 2 for r in rows) / n
    bk = sum((mid(r) - r[8]) ** 2 for r in rows) / n
    print(f"\n{n} market-samples at {decision_hour:02d}:00Z on the market day")
    print(f"Brier model {bm:.4f}   Brier market {bk:.4f}   (lower is better)")

    # simple trading rule
    pnl, fees, nbuy, nsell, wins = 0.0, 0.0, 0, 0, 0
    by_day = defaultdict(float)
    for r in rows:
        bid, ask, fair, y = r[5], r[6], r[7], r[8]
        if fair - ask > edge and ask < 0.97:
            f = kalshi_fee(ask)
            p = (y - ask) - f
            pnl += p; fees += f; nbuy += 1; wins += p > 0; by_day[r[0]] += p
        elif bid - fair > edge and bid > 0.03:
            f = kalshi_fee(bid)
            p = (bid - y) - f
            pnl += p; fees += f; nsell += 1; wins += p > 0; by_day[r[0]] += p
    nt = nbuy + nsell
    print(f"rule: edge>{edge:.2f}: {nt} trades ({nbuy} buy YES, {nsell} buy NO), win {wins / max(nt, 1):.0%}, "
          f"PnL per contract ${pnl / max(nt, 1):+.4f}, total ${pnl:+.2f} per 1-lot, fees ${fees:.2f}")
    daily = sorted(by_day.items())
    if daily:
        vals = [v for _, v in daily]
        m_, s_ = statistics.mean(vals), (statistics.pstdev(vals) if len(vals) > 1 else 0)
        print(f"per-day PnL: mean ${m_:+.3f} sd ${s_:.3f} t={m_ / (s_ / math.sqrt(len(vals))) if s_ else float('nan'):.2f} over {len(vals)} days")

    # calibration by fair bucket
    print("\nfair-bucket   n   fair   mkt   yes%")
    bins = defaultdict(list)
    for r in rows:
        bins[min(int(r[7] * 10), 9)].append(r)
    for k in sorted(bins):
        rs = bins[k]
        print(f"{k / 10:.1f}-{(k + 1) / 10:.1f}   {len(rs):4d}  {sum(r[7] for r in rs) / len(rs):.3f}  {sum(mid(r) for r in rs) / len(rs):.3f}  {sum(r[8] for r in rs) / len(rs):.3f}")


if __name__ == "__main__":
    main()
