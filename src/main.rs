mod db_controllers;
mod handler;
mod manip;
mod types;
mod utils;

use crate::db_controllers::{Database, del_database};
use crate::handler::message_handler;
use crate::types::Cli;
use crate::utils::init_tracing;

use clap::Parser;
use futures::StreamExt;
use tokio::sync::{Semaphore, mpsc};
use tokio::time::{Duration, interval};
use tracing::Level;

use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info};

use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cli = Cli::parse();

    let level = match cli.verbose {
        0 => Level::INFO,
        1 => Level::DEBUG,
        _ => Level::TRACE,
    };

    init_tracing(level);

    let cli = Arc::new(cli);

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

    info!("Market data client is starting...");

    let stream_path = cli
        .sym
        .iter()
        .map(|s| format!("{s}@depth20@100ms"))
        .collect::<Vec<_>>()
        .join("/");

    let url = format!("wss://fstream.binance.com/stream?streams={}", stream_path);

    info!("Connecting to: {}", url);
    let (ws_stream, _) = connect_async(&url).await.expect("WebSocket connect failed");
    let (_write, mut read) = ws_stream.split();

    info!("Connected to Binance WebSocket.");
    let start = Instant::now();

    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(60));
        loop {
            ticker.tick().await;

            let uptime = start.elapsed();
            let secs = uptime.as_secs();
            let hrs = secs / 3600;
            let mins = (secs % 3600) / 60;
            let sec = secs % 60;

            info!(
                "Heartbeat: system running (uptime: {}h {}m {}s)",
                hrs, mins, sec
            );
        }
    });

    // bounded channel to avoid unbounded backlog
    let (tx, mut rx) = mpsc::channel::<String>(10_000);

    // concurrency limit for DB writes (adjust to your DB capacity)
    let concurrency_limit = 8usize;
    let sem = Arc::new(Semaphore::new(concurrency_limit));

    // clone handles for dispatcher
    let db_for_dispatch = db.clone();
    let cli_for_dispatch = cli.clone();
    let sem_for_dispatch = sem.clone();

    // dispatcher: receives messages from queue and spawns worker tasks (bounded by semaphore)
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let permit = match sem_for_dispatch.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    error!("Semaphore closed, shutting dispatcher");
                    break;
                }
            };
            let db_worker = db_for_dispatch.clone();
            let cli_worker = cli_for_dispatch.clone();
            // spawn a task to process this message; permit held until task ends
            tokio::spawn(async move {
                // keep the permit in scope so it is released on drop
                let _permit = permit;
                if let Err(err) = message_handler(&db_worker, &*cli_worker, &msg).await {
                    error!("Message handling error (worker): {}", err);
                }
            });
        }
        info!("Dispatcher exiting (rx closed)");
    });

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                match tx.try_send(text) {
                    Ok(_) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        // queue is full: drop the message and log
                        error!("Inbound queue full — dropping websocket message");
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        error!("Inbound queue closed — stopping reader");
                        break;
                    }
                }
            }
            Ok(Message::Close(_)) => {
                info!("Connection closed.");
                break;
            }
            Ok(Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_)) => {}
            Err(e) => error!("WebSocket error: {}", e),
        }
    }
}
