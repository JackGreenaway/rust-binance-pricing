use crate::utils::i64_to_ts;
use chrono::Utc;
use sqlx::{Executor, PgPool};
use std::fs;
use tracing::{info, trace};

use crate::types::{AggTradeData, DepthUpdateData};

#[allow(dead_code)]
#[derive(Clone)]
pub struct Database {
    pub pool: PgPool,
}

pub async fn del_database(db_name: &str) -> Result<(), sqlx::Error> {
    let pool: sqlx::Pool<sqlx::Postgres> =
        PgPool::connect("postgres://postgres:admin@localhost:5432/postgres").await?;
    sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db_name))
        .execute(&pool)
        .await?;
    info!("Database '{}' deleted.", db_name);
    Ok(())
}

impl Database {
    fn read_sql_file(path: &str) -> String {
        fs::read_to_string(path).unwrap_or_else(|_| panic!("Failed to read SQL file: {}", path))
    }

    async fn execute_sql_file(pool: &PgPool, path: &str) -> Result<(), sqlx::Error> {
        let sql: String = Self::read_sql_file(path);

        for stmt in sql.split(';') {
            let stmt = stmt.trim();
            if !stmt.is_empty() {
                pool.execute(stmt).await?;
            }
        }

        Ok(())
    }

    pub async fn connect(db_name: &str) -> Result<Self, sqlx::Error> {
        let default_conninfo = "postgres://postgres:admin@localhost:5432/postgres";
        let pool = PgPool::connect(default_conninfo).await?;

        let db_exists: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM pg_database WHERE datname = $1")
                .bind(db_name)
                .fetch_one(&pool)
                .await?;

        if db_exists.0 == 0 {
            info!("Database '{}' does not exist. Creating...", db_name);
            sqlx::query(&format!("CREATE DATABASE {}", db_name))
                .execute(&pool)
                .await?;
        } else {
            // info!("Database '{}' already exists.", db_name);
        }

        let db_conninfo: String = format!("postgres://postgres:admin@localhost:5432/{}", db_name);
        let db_pool: sqlx::Pool<sqlx::Postgres> = PgPool::connect(&db_conninfo).await?;

        info!("Connected to '{}'", db_name);

        Ok(Database { pool: db_pool })
    }

    pub async fn create_tables(&self, sql_path: &str) -> Result<(), sqlx::Error> {
        Self::execute_sql_file(&self.pool, sql_path).await?;

        info!("Tables created successfully.");

        Ok(())
    }

    pub async fn insert_trade(&self, data: &AggTradeData) -> Result<(), sqlx::Error> {
        let num_trades: i64 = data.l - data.f + 1;
        let utc_dt: chrono::DateTime<Utc> = i64_to_ts(data.t, "utc").with_timezone(&Utc);

        sqlx::query("INSERT INTO market_trade (ts, symbol, price, quantity, num_trades, maker) VALUES ($1, $2, $3, $4, $5, $6)")
            .bind(utc_dt)
            .bind(&data.s)
            .bind(data.p)
            .bind(data.q)
            .bind(num_trades)
            .bind(data.m)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn insert_book_update(&self, data: &DepthUpdateData) -> Result<(), sqlx::Error> {
        let event_time: chrono::DateTime<Utc> = i64_to_ts(data.e2, "utc").with_timezone(&Utc);
        let transaction_time: chrono::DateTime<Utc> = i64_to_ts(data.t, "utc").with_timezone(&Utc);

        let update_id: i64 = sqlx::query_scalar(
            "
            INSERT INTO
                orderbook_updates (
                    event_time,
                    transaction_time,
                    symbol,
                    first_update_id,
                    last_update_id,
                    previous_update_id
                )
            VALUES
                ($1, $2, $3, $4, $5, $6)
            RETURNING
                ob_update_id",
        )
        .bind(event_time)
        .bind(transaction_time)
        .bind(&data.s)
        .bind(data.u)
        .bind(data.u2)
        .bind(data.p)
        .fetch_one(&self.pool)
        .await?;

        trace!(
            "Inserted ob update for event time {} @ {} for symbol: {}",
            event_time,
            Utc::now(),
            data.s
        );

        let bid_values: Vec<String> = data
            .b
            .iter()
            .enumerate()
            .map(|(i, bid)| format!("({}, {}, {}, {}, {})", update_id, 1, i + 1, bid[0], bid[1]))
            .collect();
        let bid_sql = format!(
            "INSERT INTO orderbook_levels (ob_update_id, side, level_id, price, quantity) VALUES {}",
            bid_values.join(", ")
        );
        sqlx::query(&bid_sql).execute(&self.pool).await?;

        trace!(
            "Completed bid orderbook level insert for ui# {} @ {} for symbol: {}",
            update_id,
            Utc::now(),
            data.s
        );

        let ask_values: Vec<String> = data
            .a
            .iter()
            .enumerate()
            .map(|(i, ask)| format!("({}, {}, {}, {}, {})", update_id, -1, i + 1, ask[0], ask[1]))
            .collect();
        let ask_sql = format!(
            "INSERT INTO orderbook_levels (ob_update_id, side, level_id, price, quantity) VALUES {}",
            ask_values.join(", ")
        );
        sqlx::query(&ask_sql).execute(&self.pool).await?;

        trace!(
            "Completed ask orderbook level insert for ui# {} @ {} for symbol: {}",
            update_id,
            Utc::now(),
            data.s
        );

        Ok(())
    }
}
