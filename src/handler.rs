use crate::data_manip::Orderbook;
use crate::db_controller::Database;
use crate::types::{AggTrade, BookTickerUpdate, Cli, DepthUpdate, MarkPriceUpdate};
use crate::utils::i64_to_ts;
use serde_json::Value;

use chrono::Utc;
use tracing::{debug, trace};

pub async fn message_handler(db: &Database, cli: &Cli, raw: &str) -> anyhow::Result<()> {
    let ws_msg: Value = serde_json::from_str(raw)?;
    let msg_type = ws_msg["data"]["e"].as_str().unwrap_or("");

    match msg_type {
        "aggTrade" => {
            let agg_trade: AggTrade = serde_json::from_str(raw)?;

            db.insert_trade(&agg_trade.data).await?;

            let user_dt = i64_to_ts(agg_trade.data.t, &cli.tz).format("%Y-%m-%d %H:%M:%S%.3f");
            let num_trades = agg_trade.data.l - agg_trade.data.f + 1;

            trace!(
                "Msg: {}, Ts: {}, Price: {}, Quantity: {}, Maker: {}, # Trades {}",
                msg_type, user_dt, agg_trade.data.p, agg_trade.data.q, agg_trade.data.m, num_trades
            );
        }
        "depthUpdate" => {
            let depth_update: DepthUpdate = serde_json::from_str(raw)?;

            db.insert_book_update(&depth_update.data).await?;

            let mut ob = Orderbook::from_depth_update(&depth_update.data)?;
            let _depth = ob.calculate_depth()?;

            let dt = i64_to_ts(depth_update.data.e2, &cli.tz).format("%Y-%m-%d %H:%M:%S%.3f");

            debug!(
                "Depth update @ {} for {} complete @ {}",
                depth_update.data.s,
                dt,
                Utc::now()
            );
            // trace!("{:#?}", &depth);
        }
        "bookTicker" => {
            let book_update: BookTickerUpdate = serde_json::from_str(raw)?;

            let dt = i64_to_ts(book_update.data.e2, &cli.tz).format("%Y-%m-%d %H:%M:%S%.3f");
            let a = book_update.data.a;
            let b = book_update.data.b;
            let ba_spread = ((a - b) / a) * 10_000.0;
            let mid_price = (a + b) / 2.0;

            trace!(
                "Msg: {}, Ts: {}, Bid: {:.8}, Ask: {:.8}, Spread: {:.8}, Mid: {:.8}, Quote Size [{}, {}]",
                msg_type, dt, b, a, ba_spread, mid_price, book_update.data.bq, book_update.data.aq
            );
        }
        "markPriceUpdate" => {
            let mark_price: MarkPriceUpdate = serde_json::from_str(raw)?;

            let dt = i64_to_ts(mark_price.data.e2, &cli.tz).format("%Y-%m-%d %H:%M:%S");

            trace!(
                "Msg: {}, Timestamp: {}, Symbol: {}, Price: {}, Rate: {}",
                msg_type, dt, mark_price.data.s, mark_price.data.p, mark_price.data.r
            );
        }
        _ => {}
    }

    Ok(())
}

// pub async fn message_handler(db: &Database, cli: &Cli, raw: &str) -> anyhow::Result<()> {
//     let tick_data: Value = serde_json::from_str(raw)?;
//     let msg_type = tick_data["data"]
//         .as_object()
//         .and_then(|m| m.get("e"))
//         .and_then(|v| v.as_str())
//         .unwrap_or("");

//     if msg_type.is_empty() {
//         trace!("Received unknown or malformed message: {}", raw);
//         return Ok(());
//     }

//     match msg_type {
//         "depthUpdate" => {
//             let depth_update: DepthUpdate = serde_json::from_str(raw)?;

//             db.insert_book_update(&depth_update.data).await?;

//             let mut ob = Orderbook::from_depth_update(&depth_update.data)?;
//             let _depth = ob.calculate_depth()?;

//             let dt = i64_to_ts(depth_update.data.e2, &cli.tz).format("%Y-%m-%d %H:%M:%S%.3f");

//             debug!(
//                 "Depth update @ {} for {} complete @ {}",
//                 depth_update.data.s,
//                 dt,
//                 Utc::now()
//             );
//             // trace!("{:#?}", &depth);
//         }
//         "aggTrade" => {
//             let agg_trade: AggTrade = serde_json::from_str(raw)?;

//             db.insert_trade(&agg_trade.data).await?;

//             let user_dt = i64_to_ts(agg_trade.data.t, &cli.tz).format("%Y-%m-%d %H:%M:%S%.3f");
//             let num_trades = agg_trade.data.l - agg_trade.data.f + 1;

//             trace!(
//                 "Msg: {}, Ts: {}, Price: {}, Quantity: {}, Maker: {}, # Trades {}",
//                 msg_type, user_dt, agg_trade.data.p, agg_trade.data.q, agg_trade.data.m, num_trades
//             );
//         }
//         "bookTicker" => {
//             let book_update: BookTickerUpdate = serde_json::from_str(raw)?;

//             let dt = i64_to_ts(book_update.data.e2, &cli.tz).format("%Y-%m-%d %H:%M:%S%.3f");
//             let a = book_update.data.a;
//             let b = book_update.data.b;
//             let ba_spread = ((a - b) / a) * 10_000.0;
//             let mid_price = (a + b) / 2.0;

//             trace!(
//                 "Msg: {}, Ts: {}, Bid: {:.8}, Ask: {:.8}, Spread: {:.8}, Mid: {:.8}, Quote Size [{}, {}]",
//                 msg_type, dt, b, a, ba_spread, mid_price, book_update.data.bq, book_update.data.aq
//             );
//         }
//         "markPriceUpdate" => {
//             let mark_price: MarkPriceUpdate = serde_json::from_str(raw)?;

//             let dt = i64_to_ts(mark_price.data.e2, &cli.tz).format("%Y-%m-%d %H:%M:%S");

//             trace!(
//                 "Msg: {}, Timestamp: {}, Symbol: {}, Price: {}, Rate: {}",
//                 msg_type, dt, mark_price.data.s, mark_price.data.p, mark_price.data.r
//             );
//         }
//         _ => {}
//     }
//     Ok(())
// }
