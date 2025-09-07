use serde::Deserialize;

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
    pub b: Vec<[String; 2]>,
    pub a: Vec<[String; 2]>,
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
    pub b: String,
    #[serde(rename = "B")]
    pub bq: String,
    pub a: String,
    #[serde(rename = "A")]
    pub aq: String,
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
    pub p: String,
    #[serde(rename = "P")]
    pub p2: String,
    pub i: String,
    pub r: String,
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
    pub p: String,
    pub q: String,
    pub f: i64,
    pub l: i64,
    #[serde(rename = "T")]
    pub t: i64,
    pub m: bool,
}
