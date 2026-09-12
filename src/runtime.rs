//! Process supervision: crash points, shutdown, reconnect, PostgreSQL connections.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use postgres_replication::LogicalReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};

use crate::config::env_or;

// ============================================================
// FAULT INJECTION
// ============================================================

/// A point in the commit path at which the connector can be told to die.
///
/// The four values are not arbitrary: they are the boundaries between the
/// steps whose ordering the whole design rests on. Crashing between two of
/// them puts the system in one of the states Phases 2 and 3 make claims about,
/// which is the only way to find out whether those claims hold.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum CrashPoint {
    /// A batch of the initial snapshot is durable in Feldera and the snapshot
    /// as a whole is not finished. The target now holds part of a table, which
    /// is a state no stream can be applied to, so this is the one crash the
    /// slot's position cannot describe and the reason a state file exists.
    DuringSnapshot,
    /// The batch is built and nothing has been sent. Feldera has never heard
    /// of these changes and neither has PostgreSQL. Restarting should deliver
    /// them exactly once, as a first delivery.
    BeforeSend,
    /// Feldera accepted the batch but has not reported it durable. Whether the
    /// data survives is unknown, which is precisely why the acknowledgement is
    /// not allowed to have happened yet.
    AfterSend,
    /// Feldera reported the batch durable and PostgreSQL has not been told.
    /// This is the canonical duplicate window and the reason Phase 3 exists:
    /// the restart will redeliver changes Feldera already holds.
    AfterDurable,
    /// The standby status update has been written to the socket. Whether
    /// PostgreSQL processed it before the process died is a race, so this can
    /// land as either a clean run or another duplicate.
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
    /// Which batch to die on. Dying on the first one is a weaker test than
    /// dying on a later one, because it never exercises a restart that resumes
    /// mid-stream.
    ///
    /// Batches, not transactions. The two coincide only while every transaction
    /// fits one request; once a large one is split, or several small ones are
    /// coalesced, a batch is the unit that is actually sent and therefore the
    /// only unit these crash points can be positioned against.
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

/// Die, if this is the configured point and batch.
///
/// `std::process::exit` is deliberate. A panic would unwind and run
/// destructors, and a crash does not get to do that — the whole question being
/// asked is what survives when no cleanup runs. The connector holds no local
/// durable state, so this is as faithful as an in-process kill can be; the
/// only thing a real SIGKILL would additionally skip is the flush of stdout,
/// which carries no correctness weight.
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

/// Upper bound on the gap between status updates.
///
/// Not cosmetic. PostgreSQL terminates a walsender that has not heard from its
/// standby within `wal_sender_timeout`, so this interval is what keeps the
/// connection alive on an idle database. It matches PostgreSQL's own
/// `wal_receiver_status_interval` default, which exists for the same reason.
const STATUS_INTERVAL_MAX: Duration = Duration::from_secs(10);

/// Lower bound, so that an absurd `wal_sender_timeout` cannot turn the
/// connector into a source of load in its own right.
const STATUS_INTERVAL_MIN: Duration = Duration::from_millis(500);

pub(crate) const RECONNECT_MIN: Duration = Duration::from_millis(200);
pub(crate) const RECONNECT_MAX: Duration = Duration::from_secs(30);

/// How long the connector keeps retrying without making progress before it
/// gives up and exits nonzero.
///
/// Retrying forever looks like the safe default and is not. A connector that is
/// up but not advancing holds the slot, and the slot holds WAL, so an infinite
/// retry converts a Feldera outage into a full disk on the PostgreSQL primary.
/// Exiting hands the decision to something that can escalate.
pub(crate) const RECONNECT_GIVE_UP_DEFAULT_SECS: &str = "300";

/// How long to wait for PostgreSQL to record the final acknowledgement before
/// giving up and exiting anyway.
///
/// Bounded on purpose. An unbounded graceful shutdown is a hang, and the
/// orchestrator that sent SIGTERM will send SIGKILL shortly regardless, so an
/// unbounded wait buys the ungraceful exit it was trying to avoid and adds a
/// delay first.
pub(crate) const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(5);

/// Replication position, which outlives any single connection.
///
/// Carrying `flush` across a reconnect is deliberate. PostgreSQL resumes from
/// what it durably recorded, which can be behind what Feldera already made
/// durable, so keeping the higher local figure lets the next status update
/// close the gap instead of waiting to reprocess the replayed transactions.
#[derive(Default)]
pub(crate) struct Position {
    pub(crate) received: u64,
    pub(crate) flush: u64,
}

/// Why a replication session ended.
pub(crate) enum SessionEnd {
    /// The connection dropped or the stream finished. Routine; reconnect.
    Disconnected(String),
    /// A signal arrived and the final status update has been written.
    Shutdown(&'static str),
}

/// How a wait for durability ended.
///
/// Separate from `SessionEnd` because one of the three outcomes is a success
/// and the other two are not, and the caller has work to do in the success case
/// that the waiter knows nothing about. Returning the reason rather than
/// returning `SessionEnd` directly also keeps the final status update in one
/// place: the waiter never calls `finish`, it reports that a signal arrived and
/// lets the loop it interrupted do the shutdown it already knows how to do.
pub(crate) enum WaitOutcome {
    /// Feldera made the batch durable, after this long.
    Durable(Duration),
    /// The sink is unreachable or the replication connection broke.
    Disconnected(String),
    /// A signal arrived mid-wait. The batch was never acknowledged, so
    /// abandoning it costs redelivery and nothing else.
    Shutdown(&'static str),
}

/// SIGINT and SIGTERM, installed once and shared across reconnections.
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

    /// Resolve when either signal arrives.
    ///
    /// Cancel safe, which it has to be: this sits in a `select!` beside the
    /// replication stream, so it is dropped and rebuilt on every message that
    /// wins the race. `Signal::recv` keeps its state in the `Signal` rather
    /// than in the future, so an abandoned poll loses nothing.
    pub(crate) async fn requested(&mut self) -> &'static str {
        tokio::select! {
            _ = self.interrupt.recv() => "SIGINT",
            _ = self.terminate.recv() => "SIGTERM",
        }
    }
}

/// Full jitter: a random duration in `[0, delay]`.
///
/// Every connector attached to one PostgreSQL loses it at the same instant, so
/// undelayed retries arrive together and the reconnect storm becomes a second
/// outage. The randomness only has to decorrelate peers, so the clock's low
/// bits are an adequate source and save taking a dependency for it.
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

/// Open a connection and put its driver on the runtime.
///
/// `tokio_postgres::connect` hands back two halves: a `Client` that queues
/// requests and a connection future that is the only thing that touches the
/// socket. Nothing happens until that future is polled, so the handle comes
/// back with the client rather than being detached — a caller that finishes
/// with the client can drop it and await the driver, which is how a connection
/// gets closed instead of merely abandoned. That matters most for the
/// replication connection that owns an exported snapshot, where "still open"
/// is the difference between a usable snapshot and a vanished one.
pub(crate) async fn connect_pg(
    connection_string: &str,
) -> Result<(Client, tokio::task::JoinHandle<()>), Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;

    let handle = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("PostgreSQL connection error: {error}");
        }
    });

    Ok((client, handle))
}

/// Wait until PostgreSQL has recorded `target` as the slot's flush position.
///
/// The replication protocol carries no acknowledgement of an acknowledgement:
/// standby status updates travel one way and the server never replies. A
/// process that sends one and exits immediately has no idea whether it landed,
/// and measurement says it does not — zero times out of eight. The only way to
/// find out is to look at the slot over a separate connection.
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

/// Derive the status update interval from the server's own patience.
///
/// A hardcoded interval is a hidden coupling to a setting this program does not
/// own, and the failure it produces is worse than a dropped connection.
/// Measured with `wal_sender_timeout = 4s` against a ten second interval: the
/// server closed the connection four seconds into a Feldera wait, the timer
/// fired at ten seconds and found it already gone, and the connector
/// reconnected, replayed the same transaction and did the same thing again. Up,
/// busy, and permanently at zero progress — which is the worse half of the
/// safety/liveness split, because no alert fires for it.
///
/// Dividing by three rather than two leaves room for one report to be late or
/// lost before the server gives up on us.
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

    // Zero disables the timeout, so nothing constrains the interval. Report at
    // the ceiling anyway: the position still has to reach the server for the
    // slot to release WAL, timeout or no timeout.
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
