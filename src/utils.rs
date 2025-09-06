use chrono::{DateTime, FixedOffset, Local, Offset, Utc};

pub fn i64_to_ts(ts_ms: i64, tz: &str) -> DateTime<FixedOffset> {
    let secs: i64 = ts_ms / 1000;
    let nsecs: u32 = ((ts_ms % 1000) * 1_000_000) as u32;

    let utc_dt: DateTime<Utc> = DateTime::<Utc>::from_timestamp(secs, nsecs).expect("invalid ts");

    // utc_dt.with_timezone(tz)

    match tz {
        "local" => utc_dt.with_timezone(&Local::now().offset().fix()),
        _ => utc_dt.with_timezone(&Utc.fix()),
    }
}
