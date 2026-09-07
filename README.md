# MarketsBot

Rust trading system for prediction markets (Kalshi first, Polymarket as a data / arbitrage
source). One `Strategy` trait runs unchanged in the **backtester**, the **paper trader** and
(later) **live execution**, so nothing gets deployed that hasn't been replayed against real data.

## Why this design

* **Kalshi is the primary venue.** CFTC-regulated, legal for US persons, clean REST + WebSocket
  API with RSA-PSS signing, and full public trade history. Polymarket's deepest liquidity is on the
  offshore venue that blocks US persons; we consume its Gamma/CLOB data read-only for
  cross-venue signals and the multi-outcome arb scanner.
* **First strategy: short-dated crypto fair value** (`KXBTC15M` — "BTC price up in next 15 mins?").
  These contracts are digital options on an observable index: the strike is the 60-second average of
  CF Benchmarks' BRTI before open, settlement is the 60-second average before close. Given spot and
  realized volatility the fair probability is a closed-form expression, and Coinbase spot ticks
  faster than the Kalshi book reprices. That is a *modeling* edge we can test, not a latency race
  against HFT arb bots. Each 15-minute market prints tens of thousands of trades, so the backtest
  has real statistical power within days of data.
* **Structural arbitrage** (`mb_strategy::arb`) — multi-outcome sets whose YES asks sum below $1,
  and Kalshi↔Polymarket pairs — is implemented as pure detectors plus a `mbot arb-scan` utility.
  It is the second candidate once live book recording shows how often gaps survive fees + latency.
* **Speed where it matters.** Everything is Rust on tokio: fixed-point prices (no floats on the hot
  path), `BTreeMap` books, zero-copy-ish JSON parsing, and Parquet (zstd) storage that Python/polars
  can read for research. Release profile is `lto = "fat"`.

## Layout

```
crates/core        Fp fixed-point, Orderbook, MarketEvent, FeeModel, Strategy/Context traits
crates/kalshi      REST (public + signed), WebSocket (dynamic subscriptions), order types
crates/polymarket  Gamma metadata, CLOB books/history, market WebSocket (read-only)
crates/coinbase    1-min candles (history) + ticker WebSocket (live reference price)
crates/data        Parquet rows + Recorder (hourly files under data/<kind>/<date>/)
crates/strategy    fair_value (digital option pricing), vol (EWMA realized vol), btc15m, arb
crates/backtest    SimExchange (tape & book fill models, latency), Backtester, Report, history loader
crates/cli         `mbot` binary
strategies/        TOML strategy configs
```

## Quick start

```bash
cp .env.example .env            # optional: add Kalshi API keys for WebSocket books / trading
cargo build --release
alias mbot=target/release/mbot

# 1. Pull 7 days of KXBTC15M settled markets + full trade tape + BTC-USD 1-min candles
mbot fetch-history --series KXBTC15M --days 7

# 2. Backtest, sweeping the edge threshold
mbot backtest --series KXBTC15M --edges 0.02,0.03,0.05 --latency-ms 250

# 3. Paper-trade live (works without keys via 1 Hz REST polling; keys => full WS books)
mbot paper --series KXBTC15M --record data/live

# Utilities
mbot markets --series KXBTC15M
mbot book KXBTC15M-26SEP071445-45          # needs keys
mbot arb-scan --events 200 --min-profit 0.01
mbot collect --series KXBTC15M --series KXETH15M --coinbase BTC-USD --coinbase ETH-USD
```

Logging: `RUST_LOG=debug mbot …` (per-crate: `RUST_LOG=mb_kalshi=trace,info`).

## Backtest fidelity — read this before trusting a number

Kalshi's REST history is a **trade tape**, not orderbook snapshots. In `--mode tape` the simulator
infers the touch from the tape (last YES-taker print = ask, last NO-taker print = bid, each with the
printed size, expiring after `--touch-ttl-ms`) and only lets IOC orders consume the size that
actually printed. Reference prices come from 1-minute Coinbase candles (open + close), so intra-minute
moves are invisible to the model — this is the *coarse* backtest.

`mbot collect` / `mbot paper --record` write real books (with keys) and tick-level Coinbase prices.
Once a few days of that exist, `mbot backtest --mode book --data data/live` replays exact books.
Treat tape-mode results as a go/no-go screen, book-mode results as the real estimate, and paper
trading as the final gate before any capital.

## Fees

Kalshi: `fee = ceil_to_cent(0.07 × contracts × P × (1−P))` per fill for taker; series with
`fee_type = quadratic_with_maker_fees` charge makers `0.0175 × …`. The series' `fee_multiplier`
from the API scales both. Polymarket: taker-only `rate × C × P × (1−P)` with `rate` per market
category (crypto 0.07, sports 0.05, politics 0.04, geopolitics 0). All encoded in `mb_core::FeeModel`.

## Roadmap

1. ✅ Scaffold, venue clients, Parquet storage, fee models, fair-value strategy, backtester, paper trader
2. Collect live books for KXBTC15M/KXETH15M for ~1 week; run book-mode backtests; tune `min_edge`, vol estimator, sizing
3. Live executor for Kalshi (`create_order` is implemented in `mb_kalshi::rest`; the engine wrapper with kill-switch / position reconciliation is next) — start in the **demo** environment
4. Arb engine: automate `arb-scan` findings on Polymarket multi-outcome sets; Polymarket order signing (EIP-712)
5. Maker mode: rest quotes around fair value instead of taking (zero maker fee on most Kalshi series)

## Safety

* `.env`, `*.pem` and `data/` are git-ignored. Never commit keys.
* Default `KALSHI_ENV=demo`. Nothing in this repo places live orders yet; when it does it will
  require an explicit flag and prod keys.
* State law on prediction markets is in flux — check your jurisdiction.
