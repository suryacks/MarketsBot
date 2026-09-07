use anyhow::Result;
use clap::Args as ClapArgs;
use mb_core::{BookSide, FeeModel, Fp};
use mb_kalshi::{KalshiClient, MarketsQuery};
use mb_polymarket::{ClobClient, GammaClient};
use mb_strategy::arb::{cross_venue_arb, multi_outcome_arb, ArbSignal};
use tracing::info;

#[derive(ClapArgs, Debug)]
pub struct MarketsArgs {
    #[arg(long, default_value = "KXBTC15M")]
    pub series: String,
    #[arg(long, default_value = "open")]
    pub status: String,
    #[arg(long, default_value_t = 20)]
    pub limit: usize,
}

pub async fn markets(a: MarketsArgs) -> Result<()> {
    let c = KalshiClient::from_env()?;
    let ms = c
        .get_all_markets(&MarketsQuery {
            series_ticker: Some(a.series.clone()),
            status: Some(a.status.clone()),
            ..Default::default()
        })
        .await?;
    println!("{:<34} {:>10} {:>7} {:>7} {:>12} {:>8}  close", "ticker", "strike", "bid", "ask", "volume", "result");
    for m in ms.iter().take(a.limit) {
        println!(
            "{:<34} {:>10} {:>7} {:>7} {:>12} {:>8}  {}",
            m.ticker,
            m.floor_strike.map(|s| format!("{s:.2}")).unwrap_or_default(),
            m.yes_bid_dollars.map(|p| p.fmt_dec(3)).unwrap_or_default(),
            m.yes_ask_dollars.map(|p| p.fmt_dec(3)).unwrap_or_default(),
            m.volume_fp.map(|v| v.fmt_dec(0)).unwrap_or_default(),
            m.result,
            m.close_time.map(|t| t.to_rfc3339()).unwrap_or_default()
        );
    }
    println!("({} markets)", ms.len());
    Ok(())
}

#[derive(ClapArgs, Debug)]
pub struct BookArgs {
    /// Kalshi ticker, or Polymarket token id with --poly
    pub ticker: String,
    #[arg(long)]
    pub poly: bool,
    #[arg(long, default_value_t = 10)]
    pub depth: usize,
}

pub async fn book(a: BookArgs) -> Result<()> {
    let ob = if a.poly {
        ClobClient::new()?.book(&a.ticker).await?.to_orderbook()
    } else {
        KalshiClient::from_env()?.get_orderbook(&a.ticker, a.depth as u32).await?
    };
    println!("{:>10} {:>12} | {:<10} {:<12}", "bid_qty", "bid", "ask", "ask_qty");
    let bids = ob.depth(BookSide::Bid, a.depth);
    let asks = ob.depth(BookSide::Ask, a.depth);
    for i in 0..a.depth.max(1) {
        let b = bids.get(i).map(|(p, q)| format!("{:>10} {:>12}", q.fmt_dec(2), p.fmt_dec(4))).unwrap_or_else(|| format!("{:>23}", ""));
        let s = asks.get(i).map(|(p, q)| format!("{:<10} {:<12}", p.fmt_dec(4), q.fmt_dec(2))).unwrap_or_default();
        if b.trim().is_empty() && s.trim().is_empty() {
            break;
        }
        println!("{b} | {s}");
    }
    if let (Some(m), Some(s)) = (ob.mid(), ob.spread()) {
        println!("mid {} spread {}", m.fmt_dec(4), s.fmt_dec(4));
    }
    Ok(())
}

#[derive(ClapArgs, Debug)]
pub struct ArbScanArgs {
    /// How many top-volume Polymarket events to scan
    #[arg(long, default_value_t = 100)]
    pub events: u32,
    /// Minimum net profit per $1 set to report
    #[arg(long, default_value_t = 0.005)]
    pub min_profit: f64,
    /// Kalshi ticker + Polymarket YES token pairs to compare, as KALSHI_TICKER=TOKEN_ID (repeatable)
    #[arg(long = "pair")]
    pub pairs: Vec<String>,
}

pub async fn arb_scan(a: ArbScanArgs) -> Result<()> {
    let gamma = GammaClient::new()?;
    let clob = ClobClient::new()?;

    // 1. Polymarket multi-outcome (negRisk) events: Σ YES asks < 1 or Σ NO asks < N-1.
    //    Only `negRisk` events are true partitions (exactly one outcome pays). Everything
    //    else (match sub-markets, "hits $X" ladders, "by <date>" ladders) is NOT, so summing
    //    their prices is meaningless.
    let events = gamma.events(a.events, 0).await?;
    let mut scanned = 0;
    let mut found = 0;
    let now = chrono::Utc::now();
    for ev in events.iter().filter(|e| e.neg_risk && e.markets.len() >= 3) {
        let days_to_resolve = ev
            .end_date
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|d| (d.with_timezone(&chrono::Utc) - now).num_seconds() as f64 / 86_400.0)
            .unwrap_or(365.0)
            .max(1.0);
        let tokens: Vec<String> = ev.markets.iter().filter_map(|m| m.yes_token()).collect();
        if tokens.len() != ev.markets.len() {
            continue;
        }
        let books = match clob.books(&tokens).await {
            Ok(b) => b,
            Err(e) => {
                info!(event = %ev.title, error = %e, "books failed");
                continue;
            }
        };
        scanned += 1;
        let legs: Vec<(Option<Fp>, Option<Fp>)> = books
            .iter()
            .map(|b| {
                let ob = b.to_orderbook();
                (ob.best_bid().map(|x| x.0), ob.best_ask().map(|x| x.0))
            })
            .collect();
        let fee = ev.markets.first().map(|m| m.fee_model()).unwrap_or(FeeModel::None);
        if let Some(sig) = multi_outcome_arb(&legs, &fee, a.min_profit) {
            found += 1;
            let (side, cost, fees, profit) = match sig {
                ArbSignal::BuyAllYes { cost, fees, profit } => ("BUY ALL YES", cost, fees, profit),
                ArbSignal::BuyAllNo { cost, fees, profit } => ("BUY ALL NO ", cost, fees, profit),
            };
            let ann = profit / cost * 365.0 / days_to_resolve;
            println!(
                "[POLY negRisk] {} ({} legs, {:.0}d): {side} cost {cost:.4} fees {fees:.4} => +{profit:.4}/set ({:.1}% ann.)  https://polymarket.com/event/{}",
                ev.title,
                legs.len(),
                days_to_resolve,
                ann * 100.0,
                ev.slug
            );
        }
    }
    println!("scanned {scanned} negRisk events, {found} opportunities ≥ {:.3}/set (before slippage across all legs)", a.min_profit);

    // 2. explicit Kalshi/Polymarket pairs
    if !a.pairs.is_empty() {
        let k = KalshiClient::from_env()?;
        let kfee = FeeModel::kalshi_default();
        for p in &a.pairs {
            let Some((kt, tok)) = p.split_once('=') else { continue };
            let km = k.get_market(kt).await?;
            let pb = clob.book(tok).await?.to_orderbook();
            let pfee = FeeModel::polymarket(0.04);
            let (kb, ka) = (km.yes_bid_dollars, km.yes_ask_dollars);
            let (pb_bid, pb_ask) = (pb.best_bid().map(|x| x.0), pb.best_ask().map(|x| x.0));
            println!("{kt}: kalshi {:?}/{:?}  poly {:?}/{:?}", kb, ka, pb_bid, pb_ask);
            if let (Some(ka), Some(pbb)) = (ka, pb_bid)
                && let Some(s) = cross_venue_arb(ka, &kfee, pbb, &pfee, a.min_profit)
            {
                println!("  -> buy YES kalshi @{:.3}, sell YES poly @{:.3}: +{:.4}/contract after {:.4} fees", s.buy_yes_at, s.sell_yes_at, s.profit, s.fees);
            }
            if let (Some(pa), Some(kbb)) = (pb_ask, kb)
                && let Some(s) = cross_venue_arb(pa, &pfee, kbb, &kfee, a.min_profit)
            {
                println!("  -> buy YES poly @{:.3}, sell YES kalshi @{:.3}: +{:.4}/contract after {:.4} fees", s.buy_yes_at, s.sell_yes_at, s.profit, s.fees);
            }
        }
    }
    Ok(())
}

pub async fn account() -> Result<()> {
    let c = KalshiClient::from_env()?;
    println!("balance:   {}", serde_json::to_string_pretty(&c.get_balance().await?)?);
    println!("positions: {}", serde_json::to_string_pretty(&c.get_positions().await?)?);
    Ok(())
}
