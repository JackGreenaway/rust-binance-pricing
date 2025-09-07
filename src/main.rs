mod db_controllers;
mod types;
mod utils;

use crate::db_controllers::{Database, del_database};
use crate::types::{AggTrade, BookTickerUpdate, DepthUpdate, MarkPriceUpdate};
use crate::utils::i64_to_ts;
use clap::Parser;
use futures::StreamExt;
use serde_json::Value;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[arg(short, long, default_value = "btcusdt")]
    sym: String,
    #[arg(short, long, default_value = "utc", value_parser = ["utc", "local"])]
    tz: String,
    #[arg(long, action = clap::ArgAction::SetTrue)]
    del_db: bool
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to build Tokio runtime");
    
    rt.block_on(async {
        let cli = Cli::parse();
        
        if cli.del_db {
            del_database("test_crypto_pricing").await.expect("Failed to delete DB");
        } 

        let db = Database::connect("test_crypto_pricing").await.expect("Failed to create DB");


        db.create_tables("sql/create_tables.sql").await.expect("Failed to create pricing table");


        println!("Market data client is starting....");

        let url_str: String = format!(
            "wss://fstream.binance.com/stream?streams={}@aggTrade/{}@depth10@100ms",
            // "wss://fstream.binance.com/stream?streams={}@bookTicker/{}@aggTrade/{}@depth5@100ms",
            cli.sym, 
            cli.sym
        );

        println!("Connecting to Binance WebSocket...");

        let (ws_stream, _) = connect_async(url_str).await.expect("Failed to connect");
        let (_write, mut read) = ws_stream.split();

        println!("Connected to Binance WebSocket");

        while let Some(message) = read.next().await {
            match message {
                Ok(Message::Text(text)) => {
                    let tick_data: Value = serde_json::from_str(&text).unwrap();
                    let msg_type = &tick_data["data"]["e"];

                    if msg_type == "aggTrade" {
                        let agg_trade: AggTrade = serde_json::from_str(&text).unwrap();
                        
                        db.insert_trade(&agg_trade.data).await.expect("Failed to insert trade");
                        
                        let user_dt = i64_to_ts(agg_trade.data.t, &cli.tz).format("%Y-%m-%d %H:%M:%S%.3f");
                        let num_trades= agg_trade.data.l - agg_trade.data.f + 1;
                        println!(
                            "Msg: {}, Ts: {}, Price: {}, Quantity: {}, Maker: {}, # Trades {}",
                            msg_type, user_dt, agg_trade.data.p, agg_trade.data.q, agg_trade.data.m, num_trades
                        );
                        
                    } else if msg_type == "depthUpdate" {
                        let depth_update: DepthUpdate = serde_json::from_str(&text).unwrap();

                        db.insert_book_update(&depth_update.data).await.expect("Failed to insert depth update");
                        
                        let user_dt = i64_to_ts(depth_update.data.e2, &cli.tz)
                            .format("%Y-%m-%d %H:%M:%S%.3f");
                        println!(
                            "Msg :{}, Ts: {}\nBid: {:?}\nAsk: {:?}",
                            msg_type, user_dt, depth_update.data.b, depth_update.data.a
                        )
                    } else if msg_type == "bookTicker" {
                        let book_update: BookTickerUpdate = serde_json::from_str(&text).unwrap();

                        let dt = i64_to_ts(book_update.data.e2, &cli.tz).format("%Y-%m-%d %H:%M:%S%.3f");

                        let a: f64 = book_update.data.a;
                        let b: f64 = book_update.data.b;

                        let ba_spread: f64 = ((a - b) / a) * 10_000.0;
                        let mid_price: f64 = (a + b) / 2.0;

                        println!(
                            "Msg: {}, Ts: {}, Bid: {:.8}, Ask: {:.8}, Spread: {:.8}, Mid: {:.8}, Quote Size [{}, {}]",
                            msg_type, dt, b, a, ba_spread, mid_price, book_update.data.bq, book_update.data.aq
                        )
                    } else if msg_type == "markPriceUpdate" {
                        let mark_price: MarkPriceUpdate = serde_json::from_str(&text).unwrap();

                        let dt = i64_to_ts(mark_price.data.e2, &cli.tz).format("%Y-%m-%d %H:%M:%S");

                        println!(
                            "Msg: {}, Timestamp: {}, Symbol: {}, Price: {}, Rate: {}",
                            msg_type, dt, mark_price.data.s, mark_price.data.p, mark_price.data.r
                        );
                    }
                }

                Ok(Message::Ping(_))
                | Ok(Message::Pong(_))
                | Ok(Message::Binary(_))
                | Ok(Message::Frame(_)) => {}
                Ok(Message::Close(_)) => {
                    println!("Connection closed.");
                    return;
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                }
            }
        }
    });
}
