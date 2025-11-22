use clap::Parser;
use serde::Deserialize;

fn string_to_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    s.parse::<f64>().map_err(serde::de::Error::custom)
}

fn vec_of_string_pairs_to_f64<'de, D>(deserializer: D) -> Result<Vec<[f64; 2]>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v: Vec<[String; 2]> = Deserialize::deserialize(deserializer)?;
    v.into_iter()
        .map(|[s1, s2]| {
            let f1 = s1.parse::<f64>().map_err(serde::de::Error::custom)?;
            let f2 = s2.parse::<f64>().map_err(serde::de::Error::custom)?;
            Ok([f1, f2])
        })
        .collect()
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
pub struct Cli {
    #[arg(short, long, default_value = "btcusdt", num_args=1..)]
    pub sym: Vec<String>,

    #[arg(short, long, default_value = "utc", value_parser = ["utc", "local"])]
    pub tz: String,

    #[arg(long, action = clap::ArgAction::SetTrue)]
    pub del_db: bool,

    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,
}

// fn string_to_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
// where
//     D: serde::Deserializer<'de>,
// {
//     let s: String = Deserialize::deserialize(deserializer)?;
//     s.parse::<i64>().map_err(serde::de::Error::custom)
// }

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct DepthUpdate {
    pub stream: String,
    pub data: DepthUpdateData,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct DepthUpdateData {
    pub e: String,
    #[serde(rename = "E")]
    pub e2: i64,
    #[serde(rename = "T")]
    pub t: i64,
    pub s: String,
    #[serde(rename = "U")]
    pub u: i64,
    #[serde(rename = "u")]
    pub u2: i64,
    #[serde(rename = "pu")]
    pub p: i64,
    #[serde(deserialize_with = "vec_of_string_pairs_to_f64")]
    pub b: Vec<[f64; 2]>,
    #[serde(deserialize_with = "vec_of_string_pairs_to_f64")]
    pub a: Vec<[f64; 2]>,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct BookTickerUpdate {
    pub stream: String,
    pub data: BookTickerData,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct BookTickerData {
    pub e: String,
    pub u: i64,
    #[serde(rename = "E")]
    pub e2: i64,
    #[serde(rename = "T")]
    pub t: i64,
    pub s: String,
    #[serde(deserialize_with = "string_to_f64")]
    pub b: f64,
    #[serde(rename = "B")]
    #[serde(deserialize_with = "string_to_f64")]
    pub bq: f64,
    #[serde(deserialize_with = "string_to_f64")]
    pub a: f64,
    #[serde(rename = "A")]
    #[serde(deserialize_with = "string_to_f64")]
    pub aq: f64,
}

#[derive(Deserialize)]
#[allow(dead_code)]
pub struct MarkPriceUpdate {
    pub stream: String,
    pub data: MarkPriceUpdateData,
}
#[derive(Deserialize)]
#[allow(dead_code)]
pub struct MarkPriceUpdateData {
    pub e: String,
    #[serde(rename = "E")]
    pub e2: i64,
    pub s: String,
    #[serde(deserialize_with = "string_to_f64")]
    pub p: f64,
    #[serde(rename = "P")]
    #[serde(deserialize_with = "string_to_f64")]
    pub p2: f64,
    #[serde(deserialize_with = "string_to_f64")]
    pub i: f64,
    #[serde(deserialize_with = "string_to_f64")]
    pub r: f64,
    #[serde(rename = "T")]
    pub t: i64,
}
#[derive(Deserialize)]
#[allow(dead_code)]
pub struct AggTrade {
    pub stream: String,
    pub data: AggTradeData,
}
#[derive(Deserialize)]
#[allow(dead_code)]
pub struct AggTradeData {
    pub e: String,
    #[serde(rename = "E")]
    pub e2: i64,
    pub a: i64,
    pub s: String,
    #[serde(deserialize_with = "string_to_f64")]
    pub p: f64,
    #[serde(deserialize_with = "string_to_f64")]
    pub q: f64,
    pub f: i64,
    pub l: i64,
    #[serde(rename = "T")]
    pub t: i64,
    pub m: bool,
}
