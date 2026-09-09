"""Watch the overnight runs, act only on evidence, and leave a morning report.

Deliberately does NOT retune anything on the fly. Every strategy in this repo that
looked good on a small sample (favourite-maker t=2.27 in backtest, BTC maker
t=0.73 on 30 markets) turned out to be noise or worse, and refitting hourly
against a handful of settlements is how that keeps happening. This only:

  * records what each run has actually settled, with a t-statistic
  * stops a LIVE run whose realized loss is statistically real (t <= -2), not merely
    unlucky -- the kill switch covers size, this covers being wrong
  * writes reports/overnight.json plus a plain-language summary to read on waking

Usage: research/venv/bin/python research/overnight_supervisor.py [--every 1200]
"""
import glob, json, os, statistics, subprocess, sys, time
from datetime import datetime, timezone

STATE = "data/state"


def arg(n, d):
    return type(d)(sys.argv[sys.argv.index(n) + 1]) if n in sys.argv else d


def tstat(pnls):
    if len(pnls) < 3:
        return 0.0
    sd = statistics.stdev(pnls)
    if sd <= 0:
        return 0.0
    return statistics.mean(pnls) / (sd / len(pnls) ** 0.5)


def snapshot():
    runs = []
    for f in sorted(glob.glob(f"{STATE}/*.json")):
        try:
            s = json.load(open(f))
        except Exception:  # noqa: BLE001
            continue
        if "equity" not in s:
            continue
        stale = (time.time() * 1000 - s.get("updated_ms", 0)) / 60000
        pnls = [x["pnl"] for x in s.get("settled", [])]
        runs.append({
            "run_id": s.get("run_id"), "mode": s.get("mode"), "live": s.get("mode") == "LIVE",
            "equity": s.get("equity"), "baseline": s.get("initial_cash"),
            "pnl": (s.get("equity") or 0) - (s.get("initial_cash") or 0),
            "realized": sum(pnls), "settled": len(pnls), "t": round(tstat(pnls), 2),
            "fills": s.get("n_fills"), "halted": (s.get("risk") or {}).get("halted"),
            "stale_min": round(stale, 1),
        })
    return runs


def stop(run_id, why):
    subprocess.run(["pkill", "-f", run_id], check=False)
    print(f"  !! STOPPED {run_id}: {why}", flush=True)


def main():
    every = arg("--every", 1200)
    hist = []
    os.makedirs("reports", exist_ok=True)
    while True:
        now = datetime.now(timezone.utc)
        runs = snapshot()
        hist.append({"ts": now.isoformat(), "runs": runs})
        live = [r for r in runs if r["live"] and r["stale_min"] < 5]
        print(f"\n=== {now:%H:%M} UTC ===", flush=True)
        for r in sorted(runs, key=lambda x: (not x["live"], x["run_id"] or "")):
            # A stopped run's file keeps its last numbers forever. Hiding it is the
            # difference between "two live runs" and an overnight report that implies four.
            if r["stale_min"] > 30:
                continue
            tag = "LIVE" if r["live"] else "papr"
            print(f"  {tag} {r['run_id']:<26} pnl {r['pnl']:>+7.2f} realized {r['realized']:>+7.2f} "
                  f"settled {r['settled']:>3} t {r['t']:>5} fills {r['fills']:>4}"
                  + ("  HALTED" if r["halted"] else ""), flush=True)
        # Act only where the loss is statistically real, not merely negative.
        for r in live:
            if r["settled"] >= 12 and r["t"] <= -2.0 and r["realized"] < 0:
                stop(r["run_id"], f"realized {r['realized']:+.2f} over {r['settled']} markets, t={r['t']} — a real loser, not variance")
        with open("reports/overnight.json", "w") as f:
            json.dump({"kind": "overnight", "updated": now.isoformat(), "history": hist[-200:]}, f, indent=1)
        time.sleep(every)


if __name__ == "__main__":
    main()
