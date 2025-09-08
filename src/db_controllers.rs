use crate::utils::i64_to_ts;
use chrono::Utc;
use sqlx::{Executor, PgPool};
use std::fs;

use crate::types::{AggTradeData, DepthUpdateData};

#[allow(dead_code)]
pub struct Database {
    pub pool: PgPool,
}

pub async fn del_database(db_name: &str) -> Result<(), sqlx::Error> {
    let pool: sqlx::Pool<sqlx::Postgres> =
        PgPool::connect("postgres://postgres:admin@localhost:5432/postgres").await?;
    sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db_name))
        .execute(&pool)
        .await?;
    println!("Database '{}' deleted.", db_name);
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
            println!("Database '{}' does not exist. Creating...", db_name);
            sqlx::query(&format!("CREATE DATABASE {}", db_name))
                .execute(&pool)
                .await?;
        } else {
            // println!("Database '{}' already exists.", db_name);
        }

        let db_conninfo: String = format!("postgres://postgres:admin@localhost:5432/{}", db_name);
        let db_pool: sqlx::Pool<sqlx::Postgres> = PgPool::connect(&db_conninfo).await?;

        println!("Connected to '{}'", db_name);

        Ok(Database { pool: db_pool })
    }

    pub async fn create_tables(&self, sql_path: &str) -> Result<(), sqlx::Error> {
        Self::execute_sql_file(&self.pool, sql_path).await?;

        println!("Tables created successfully.");

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
                depth_updates (
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
                depth_update_id",
        )
        .bind(event_time)
        .bind(transaction_time)
        .bind(&data.s)
        .bind(data.u)
        .bind(data.u2)
        .bind(data.p)
        .fetch_one(&self.pool)
        .await?;

        for (i, bid) in data.b.iter().enumerate() {
            sqlx::query(
                "INSERT INTO bid_depth (depth_update_id, level_id, price, quantity) VALUES ($1, $2, $3, $4)",
            )
            .bind(update_id)
            .bind((i + 1) as i32)
            .bind(bid[0])
            .bind(bid[1])
            .execute(&self.pool)
            .await?;
        }

        for (i, ask) in data.a.iter().enumerate() {
            sqlx::query(
                "INSERT INTO ask_depth (depth_update_id, level_id, price, quantity) VALUES ($1, $2, $3, $4)",
            )
            .bind(update_id)
            .bind((i + 1) as i32)
            .bind(ask[0])
            .bind(ask[1])
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }
}
