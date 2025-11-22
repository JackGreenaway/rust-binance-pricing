use chrono::{DateTime, FixedOffset, Local, Offset, Utc};

use tracing::Level;
use tracing_appender::rolling::RollingFileAppender;
use tracing_subscriber::{EnvFilter, Layer, Registry, fmt, layer::SubscriberExt};

static mut LOG_GUARD: Option<tracing_appender::non_blocking::WorkerGuard> = None;

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

pub fn init_tracing(level: Level) {
    let file_appender: RollingFileAppender = tracing_appender::rolling::hourly("logs", "app.log");
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    unsafe {
        LOG_GUARD = Some(guard);
    }

    let fmt_layer = fmt::layer()
        .with_writer(non_blocking)
        .with_ansi(false)
        .with_target(false)
        .with_level(true);

    let stdout_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_ansi(true)
        .with_target(false)
        .with_level(true)
        .with_filter(EnvFilter::new("info"));

    // Combine default level with sqlx=off
    let filter = EnvFilter::default()
        .add_directive(level.into())
        .add_directive("sqlx=off".parse().unwrap());

    let subscriber = Registry::default()
        .with(fmt_layer)
        .with(stdout_layer)
        .with(filter);

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    tracing::info!("Logger initialized at {level:?}");
}
