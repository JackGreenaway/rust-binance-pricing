use crate::types::DepthUpdateData;

use polars::lazy::prelude::*;
use polars::prelude::*;

pub struct Orderbook {
    df: DataFrame,
}

impl Orderbook {
    /// Construct an Orderbook from a DepthUpdateData message
    pub fn from_depth_update(update: &DepthUpdateData) -> PolarsResult<Self> {
        // let depth_update_id = update.e2;

        let mut sides = Vec::new();
        let mut prices = Vec::new();
        let mut quantities = Vec::new();
        let mut level_ids = Vec::new();

        for (i, [price, qty]) in update.b.iter().enumerate() {
            sides.push(1i32);
            prices.push(*price);
            quantities.push(*qty);
            level_ids.push(i as i32 + 1);
        }

        for (i, [price, qty]) in update.a.iter().enumerate() {
            sides.push(-1i32);
            prices.push(*price);
            quantities.push(*qty);
            level_ids.push(i as i32 + 1);
        }

        let df = df![
            "side" => sides,
            "price" => prices,
            "quantity" => quantities,
            "level_id" => level_ids,
        ]?;

        Ok(Self { df })
    }

    pub fn calculate_depth(&mut self) -> PolarsResult<DataFrame> {
        let lf: LazyFrame = self.df.clone().lazy().with_columns([col("quantity")
            .cum_sum(false)
            .over([col("side")])
            .alias("cumulative_depth")]);

        let orderbook_oos = lf.clone().filter(col("level_id").eq(lit(1))).with_columns([
            lit(0i64).alias("level_id"),
            lit(0f64).alias("quantity"),
            lit(0f64).alias("cumulative_depth"),
        ]);

        let orderbook_oos_df = orderbook_oos.clone().collect()?;
        let mid_price: f64 = orderbook_oos_df
            .column("price")?
            .f64()?
            .sum()
            .unwrap_or(0.0)
            / 2.0;

        let orderbook_depth = concat(
            &[lf, orderbook_oos],
            UnionArgs {
                rechunk: true,
                parallel: true,
                ..Default::default()
            },
        )?
        .lazy()
        .with_columns([
            ((col("price") - lit(mid_price)) / lit(mid_price) * lit(10_000f64))
                .alias("bps_from_mid"),
        ])
        .sort(vec!["price"], Default::default());

        // self.df = orderbook_depth.collect()?;

        Ok(orderbook_depth.collect()?)
    }

    #[allow(dead_code)]
    pub fn df(&self) -> &DataFrame {
        &self.df
    }
}
