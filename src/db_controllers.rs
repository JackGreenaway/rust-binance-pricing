use chrono::{DateTime, Utc};
use sqlx::{Executor, PgPool};
use std::fs;

#[allow(dead_code)]
pub struct Database {
    pub pool: PgPool,
}

impl Database {
    fn read_sql_file(path: &str) -> String {
        fs::read_to_string(path).unwrap_or_else(|_| panic!("Failed to read SQL file: {}", path))
    }

    async fn execute_sql_file(pool: &PgPool, path: &str) -> Result<(), sqlx::Error> {
        let sql = Self::read_sql_file(path);

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

        let db_conninfo = format!("postgres://postgres:admin@localhost:5432/{}", db_name);
        let db_pool = PgPool::connect(&db_conninfo).await?;

        println!("Connected to '{}'", db_name);

        Ok(Database { pool: db_pool })
    }

    pub async fn create_tables(&self, sql_path: &str) -> Result<(), sqlx::Error> {
        Self::execute_sql_file(&self.pool, sql_path).await?;

        // let sql = Self::read_sql_file(sql_path);
        // self.pool.execute(sql.as_str()).await?;

        println!("Tables created successfully.");

        Ok(())
    }

    pub async fn insert_trade(
        &self,
        ts: DateTime<Utc>,
        symbol: &String,
        price: &String,
        quantity: &String,
        maker: bool,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO market_trade (ts, symbol, price, quantity, maker) VALUES ($1, $2, $3, $4, $5)")
            .bind(ts)
            .bind(symbol)
            .bind(price)
            .bind(quantity)
            .bind(maker)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}
