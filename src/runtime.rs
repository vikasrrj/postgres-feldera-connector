//! Process supervision: crash points, shutdown, reconnect, PostgreSQL connections.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use postgres_native_tls::MakeTlsConnector;
use postgres_replication::LogicalReplicationStream;
use tokio_postgres::Client;
use tokio_postgres::types::PgLsn;

use crate::config::env_or;

// ============================================================
// FAULT INJECTION
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum CrashPoint {
    DuringSnapshot,
    BeforeSend,
    AfterSend,
    AfterDurable,
    AfterAck,
}

impl CrashPoint {
    pub(crate) fn parse(name: &str) -> Option<Self> {
        match name {
            "during-snapshot" => Some(Self::DuringSnapshot),
            "before-send" => Some(Self::BeforeSend),
            "after-send" => Some(Self::AfterSend),
            "after-durable" => Some(Self::AfterDurable),
            "after-ack" => Some(Self::AfterAck),
            _ => None,
        }
    }
}

pub(crate) struct CrashConfig {
    pub(crate) after_batches: u64,
    pub(crate) point: CrashPoint,
}

impl CrashConfig {
    pub(crate) fn from_env() -> Result<Option<Self>, Box<dyn std::error::Error>> {
        let Ok(name) = std::env::var("CRASH_AT") else {
            return Ok(None);
        };

        let point = CrashPoint::parse(&name).ok_or_else(|| {
            format!(
                "CRASH_AT must be one of during-snapshot, before-send, after-send, \
                 after-durable, after-ack; got {name:?}"
            )
        })?;

        Ok(Some(Self {
            point,
            after_batches: env_or("CRASH_AFTER_BATCHES", "1").parse::<u64>()?,
        }))
    }
}

pub(crate) fn maybe_crash(config: &Option<CrashConfig>, point: CrashPoint, batches: u64) {
    let Some(config) = config else { return };

    if config.point != point || config.after_batches != batches {
        return;
    }

    eprintln!("CRASH: injected failure at {point:?} on batch {batches}");

    std::process::exit(70);
}

/// Seconds between the Unix epoch (1970-01-01)
/// and the Postgres epoch (2000-01-01).
const POSTGRES_EPOCH_OFFSET_SECONDS: u64 = 946_684_800;

/// Microseconds since 2000-01-01 00:00:00 UTC.
pub(crate) fn postgres_timestamp_now() -> i64 {
    let postgres_epoch = UNIX_EPOCH + Duration::from_secs(POSTGRES_EPOCH_OFFSET_SECONDS);

    SystemTime::now()
        .duration_since(postgres_epoch)
        .expect("system clock is before 2000-01-01")
        .as_micros() as i64
}

pub(crate) async fn send_standby_status(
    stream: &mut std::pin::Pin<&mut LogicalReplicationStream>,
    received_lsn: u64,
    flush_lsn: u64,
    reply: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    stream
        .as_mut()
        .standby_status_update(
            PgLsn::from(received_lsn),
            PgLsn::from(flush_lsn),
            PgLsn::from(flush_lsn),
            postgres_timestamp_now(),
            reply,
        )
        .await?;

    Ok(())
}

// ============================================================
// LIVENESS
// ============================================================

const STATUS_INTERVAL_MAX: Duration = Duration::from_secs(10);

const STATUS_INTERVAL_MIN: Duration = Duration::from_millis(500);

pub(crate) const RECONNECT_MIN: Duration = Duration::from_millis(200);
pub(crate) const RECONNECT_MAX: Duration = Duration::from_secs(30);

pub(crate) const RECONNECT_GIVE_UP_DEFAULT_SECS: &str = "300";

pub(crate) const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Default)]
pub(crate) struct Position {
    pub(crate) received: u64,
    pub(crate) flush: u64,
}

pub(crate) enum SessionEnd {
    Disconnected(String),
    Shutdown(&'static str),
}

pub(crate) enum WaitOutcome {
    Durable(Duration),
    Disconnected(String),
    Shutdown(&'static str),
}

pub(crate) struct Shutdown {
    pub(crate) interrupt: tokio::signal::unix::Signal,
    pub(crate) terminate: tokio::signal::unix::Signal,
}

impl Shutdown {
    pub(crate) fn install() -> Result<Self, Box<dyn std::error::Error>> {
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    pub(crate) async fn requested(&mut self) -> &'static str {
        tokio::select! {
            _ = self.interrupt.recv() => "SIGINT",
            _ = self.terminate.recv() => "SIGTERM",
        }
    }
}

pub(crate) fn jittered(delay: Duration) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| u64::from(since.subsec_nanos()))
        .unwrap_or(0);

    let span = delay.as_millis() as u64;

    if span == 0 {
        return delay;
    }

    Duration::from_millis(nanos % (span + 1))
}

pub(crate) async fn connect_pg(
    connection_string: &str,
) -> Result<(Client, tokio::task::JoinHandle<()>), Box<dyn std::error::Error>> {
    let connector = native_tls::TlsConnector::builder().build()?;
    let tls = MakeTlsConnector::new(connector);

    let (client, connection) = tokio_postgres::connect(connection_string, tls).await?;

    let handle = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("PostgreSQL connection error: {error}");
        }
    });

    Ok((client, handle))
}

pub(crate) async fn confirm_slot_recorded(
    base_connection: &str,
    slot: &str,
    target: u64,
    deadline: Duration,
) -> Result<bool, Box<dyn std::error::Error>> {
    let (client, handle) = connect_pg(base_connection).await?;

    let started = Instant::now();

    let recorded = loop {
        let row = client
            .query_opt(
                "select confirmed_flush_lsn::text from pg_replication_slots \
                 where slot_name = $1",
                &[&slot],
            )
            .await?
            .ok_or_else(|| format!("replication slot {slot:?} no longer exists"))?;

        let confirmed = match row.try_get::<_, Option<String>>(0)? {
            Some(text) => u64::from(
                text.parse::<PgLsn>()
                    .map_err(|_| format!("could not parse LSN {text:?}"))?,
            ),
            None => 0,
        };

        if confirmed >= target {
            break true;
        }

        if started.elapsed() >= deadline {
            break false;
        }

        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    drop(client);

    let _ = handle.await;

    Ok(recorded)
}

pub(crate) async fn negotiate_status_interval(
    base_connection: &str,
) -> Result<Duration, Box<dyn std::error::Error>> {
    let (client, handle) = connect_pg(base_connection).await?;

    let row = client.query_one("show wal_sender_timeout", &[]).await?;
    let raw: String = row.try_get(0)?;

    drop(client);
    let _ = handle.await;

    let milliseconds = parse_pg_interval_ms(&raw)
        .ok_or_else(|| format!("could not read wal_sender_timeout, got {raw:?}"))?;

    // Zero disables the timeout; report at the ceiling anyway.
    if milliseconds == 0 {
        println!(
            "wal_sender_timeout is disabled; reporting position every {STATUS_INTERVAL_MAX:?}"
        );

        return Ok(STATUS_INTERVAL_MAX);
    }

    let timeout = Duration::from_millis(milliseconds);

    let interval = (timeout / 3).clamp(STATUS_INTERVAL_MIN, STATUS_INTERVAL_MAX);

    if timeout < STATUS_INTERVAL_MIN * 3 {
        eprintln!(
            "WARNING: wal_sender_timeout is {raw}, too short to report inside comfortably. \
             Reporting every {interval:?} regardless, because a slower report guarantees the \
             connection drops while a faster one only wastes work; raise wal_sender_timeout."
        );
    }

    println!("wal_sender_timeout is {raw}; reporting position every {interval:?}");

    Ok(interval)
}

/// `show` renders a GUC in whatever unit it likes: "60s", "1min", "60000ms".
pub(crate) fn parse_pg_interval_ms(raw: &str) -> Option<u64> {
    let raw = raw.trim();

    let (digits, multiplier) = if let Some(rest) = raw.strip_suffix("ms") {
        (rest, 1)
    } else if let Some(rest) = raw.strip_suffix("min") {
        (rest, 60_000)
    } else if let Some(rest) = raw.strip_suffix('s') {
        (rest, 1_000)
    } else {
        (raw, 1)
    };

    digits.trim().parse::<u64>().ok().map(|n| n * multiplier)
}
