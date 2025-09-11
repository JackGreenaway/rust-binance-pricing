use crate::db_controllers::Database;
use crate::manip::Orderbook;
use crate::types::{AggTrade, BookTickerUpdate, Cli, DepthUpdate, MarkPriceUpdate};

use chrono::{DateTime, FixedOffset, Local, Offset, Utc};
use crossterm::{
    execute,
    terminal::{Clear, ClearType},
};
use polars::lazy::prelude::*;
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Style},
    widgets::{
        // Bar,
        BarChart,
        Block,
        Borders,
    },
};
use serde_json::Value;
use std::io;

pub fn clear_cli() {
    let mut stdout = io::stdout();
    execute!(stdout, Clear(ClearType::All)).unwrap();
}

pub fn i64_to_ts(ts_ms: i64, tz: &str) -> DateTime<FixedOffset> {
    let secs: i64 = ts_ms / 1000;
    let nsecs: u32 = ((ts_ms % 1000) * 1_000_000) as u32;

    let utc_dt: DateTime<Utc> = DateTime::<Utc>::from_timestamp(secs, nsecs).expect("invalid ts");

    // utc_dt.with_timezone(tz)

    match tz {
        "local" => utc_dt.with_timezone(&Local::now().offset().fix()),
        _ => utc_dt.with_timezone(&Utc.fix()),
    }
}

pub async fn message_handler(db: &Database, cli: &Cli, raw: &str) -> anyhow::Result<()> {
    let tick_data: Value = serde_json::from_str(raw)?;
    let msg_type = tick_data["data"]["e"].as_str().unwrap_or("");

    match msg_type {
        "aggTrade" => {
            let agg_trade: AggTrade = serde_json::from_str(raw)?;

            db.insert_trade(&agg_trade.data).await?;

            let user_dt = i64_to_ts(agg_trade.data.t, &cli.tz).format("%Y-%m-%d %H:%M:%S%.3f");
            let num_trades = agg_trade.data.l - agg_trade.data.f + 1;

            println!(
                "Msg: {}, Ts: {}, Price: {}, Quantity: {}, Maker: {}, # Trades {}",
                msg_type, user_dt, agg_trade.data.p, agg_trade.data.q, agg_trade.data.m, num_trades
            );
        }
        "depthUpdate" => {
            let depth_update: DepthUpdate = serde_json::from_str(raw)?;

            db.insert_book_update(&depth_update.data).await?;

            let mut ob = Orderbook::from_depth_update(&depth_update.data)?;
            let depth = ob.calculate_depth()?;

            if let Err(e) = render_depth_chart(&depth) {
                eprintln!("Failed to render chart {}", e)
            }

            // let dt = i64_to_ts(depth_update.data.e2, &cli.tz).format("%Y-%m-%d %H:%M:%S%.3f");

            // println!("{}", dt);
            // println!("{:#?}", &depth);
        }
        "bookTicker" => {
            let book_update: BookTickerUpdate = serde_json::from_str(raw)?;

            let dt = i64_to_ts(book_update.data.e2, &cli.tz).format("%Y-%m-%d %H:%M:%S%.3f");
            let a = book_update.data.a;
            let b = book_update.data.b;
            let ba_spread = ((a - b) / a) * 10_000.0;
            let mid_price = (a + b) / 2.0;

            println!(
                "Msg: {}, Ts: {}, Bid: {:.8}, Ask: {:.8}, Spread: {:.8}, Mid: {:.8}, Quote Size [{}, {}]",
                msg_type, dt, b, a, ba_spread, mid_price, book_update.data.bq, book_update.data.aq
            );
        }
        "markPriceUpdate" => {
            let mark_price: MarkPriceUpdate = serde_json::from_str(raw)?;

            let dt = i64_to_ts(mark_price.data.e2, &cli.tz).format("%Y-%m-%d %H:%M:%S");

            println!(
                "Msg: {}, Timestamp: {}, Symbol: {}, Price: {}, Rate: {}",
                msg_type, dt, mark_price.data.s, mark_price.data.p, mark_price.data.r
            );
        }
        _ => {}
    }
    Ok(())
}

fn render_depth_chart(depth: &polars::prelude::DataFrame) -> anyhow::Result<()> {
    fn extract_side_data(
        depth: &polars::prelude::DataFrame,
        side_val: i32,
    ) -> anyhow::Result<Vec<(String, u64)>> {
        let side_data = depth
            .clone()
            .lazy()
            .filter(col("side").eq(lit(side_val)))
            .collect()?;

        let prices: Vec<String> = side_data
            .column("price")?
            .f64()?
            .into_no_null_iter()
            .map(|p| format!("{:.2}", p))
            .collect();

        let cum_depth: Vec<u64> = side_data
            .column("cumulative_depth")?
            .f64()?
            .into_no_null_iter()
            .map(|d| d as u64)
            .collect();

        Ok(prices
            .into_iter()
            .zip(cum_depth.into_iter())
            .collect::<Vec<(String, u64)>>())
    }

    let max_depth = depth
        .column("cumulative_depth")?
        .f64()?
        .into_no_null_iter()
        .fold(0.0_f64, |a, b| a.max(b));
    let n_depth_levels = (max_depth.ceil() as u64) + 3;

    let mut stdout = io::stdout();
    let backend = CrosstermBackend::new(&mut stdout);
    let mut terminal = Terminal::new(backend)?;

    let bid_vec = extract_side_data(depth, 1)?;
    let bid_vec: Vec<(&str, u64)> = bid_vec.iter().map(|(s, d)| (s.as_str(), *d)).collect();

    let ask_vec = extract_side_data(depth, -1)?;
    let ask_vec: Vec<(&str, u64)> = ask_vec.iter().map(|(s, d)| (s.as_str(), *d)).collect();

    // let best_bid = bid_vec.last().map(|(_, d)| *d as f64).unwrap_or(0.0);
    // let best_ask = ask_vec.first().map(|(_, d)| *d as f64).unwrap_or(0.0);
    // let mid_price = (best_bid + best_ask) / 2.0;

    fn plot_bar<'a>(
        title: &'a str,
        data: &'a [(&str, u64)],
        color: Color,
        n_depth_levels: u64,
    ) -> BarChart<'a> {
        BarChart::default()
            .block(Block::default().title(title).borders(Borders::ALL))
            .data(data)
            .bar_style(Style::default().fg(color))
            .bar_gap(0)
            .max(n_depth_levels)
    }

    terminal.draw(|f| {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)].as_ref())
            .split(f.area());

        let bids = plot_bar("Bids", &bid_vec, Color::Green, n_depth_levels);

        let asks = plot_bar("Asks", &ask_vec, Color::Red, n_depth_levels);

        f.render_widget(bids, chunks[0]);
        f.render_widget(asks, chunks[1]);
    })?;

    Ok(())
}
