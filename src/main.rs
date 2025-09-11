mod db_controllers;
mod manip;
mod types;
mod utils;

use crate::db_controllers::{Database, del_database};
use crate::types::Cli;
use crate::utils::{clear_cli, message_handler};

use clap::Parser;
use futures::StreamExt;

use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();

    if cli.del_db {
        del_database("test_crypto_pricing")
            .await
            .expect("Failed to delete DB");
    }

    let db = Database::connect("test_crypto_pricing")
        .await
        .expect("Failed to connect DB");

    db.create_tables("sql/create_tables.sql")
        .await
        .expect("Failed to create tables");

    println!("Market data client is starting...");
    let url = format!(
        // "wss://fstream.binance.com/stream?streams={}@aggTrade/{}@depth20@100ms",
        "wss://fstream.binance.com/stream?streams={}@depth20@100ms",
        cli.sym
    );

    println!("Connecting to Binance WebSocket...");
    let (ws_stream, _) = connect_async(&url).await.expect("WebSocket connect failed");
    let (_write, mut read) = ws_stream.split();

    println!("Connected to Binance WebSocket.");

    clear_cli();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Err(err) = message_handler(&db, &cli, &text).await {
                    eprintln!("Message handling error: {}", err);
                }
            }
            Ok(Message::Close(_)) => {
                println!("Connection closed.");
                break;
            }
            Ok(Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_)) => {}
            Err(e) => eprintln!("WebSocket error: {}", e),
        }
    }
}
