mod config;
mod connector;
mod contract;
mod decode;
mod feldera;
mod outbox;
mod runtime;
mod snapshot;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::config::env_or;
use crate::connector::Connector;
use crate::contract::{TableRoute, identifier_list, parse_table_pairs};
use crate::feldera::{AckMode, BATCH_LINGER_MS_DEFAULT, BATCH_MAX_ROWS_DEFAULT, FelderaSink};
use crate::runtime::{
    CrashConfig, Position, RECONNECT_GIVE_UP_DEFAULT_SECS, RECONNECT_MAX, RECONNECT_MIN,
    SessionEnd, Shutdown, jittered, negotiate_status_interval,
};
use crate::snapshot::{Progress, SNAPSHOT_BATCH_ROWS_DEFAULT, SnapshotMode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --------------------------------------------------------
    // Sink
    // --------------------------------------------------------

    let sink = FelderaSink::from_env()?;

    let crash = CrashConfig::from_env()?;

    if let Some(config) = &crash {
        eprintln!(
            "WARNING: fault injection armed. This process will exit at {:?} on batch {}.",
            config.point, config.after_batches
        );
    }

    match sink.ack_mode {
        AckMode::OnCompleted => {
            println!("Acknowledging PostgreSQL only after Feldera reports completion");
        }

        AckMode::OnAccepted => {
            eprintln!(
                "WARNING: FELDERA_ACK_MODE=accepted. PostgreSQL will be told data is safe as soon"
            );
            eprintln!(
                "WARNING: as Feldera buffers it, so a pipeline crash loses it permanently. This"
            );
            eprintln!("WARNING: mode exists to demonstrate that loss, not to be run for real.");
        }
    }

    // --------------------------------------------------------
    // Tables
    // --------------------------------------------------------
    //
    // One publication can name many tables. Feldera's ingress cannot: each
    // URL is one table. The list is what joins those two facts. Everything
    // downstream of Phase 2 still rests on redelivery being harmless, and
    // that is still a property of each destination table's SQL, so each
    // contract is verified here rather than trusted.

    let pairs = parse_table_pairs()?;
    let mut routes = Vec::with_capacity(pairs.len());

    for (source, feldera) in pairs {
        let contract = sink.fetch_target_contract(&feldera).await?;

        if contract.primary_key.is_empty() {
            let explanation = format!(
                "Feldera table {} has no PRIMARY KEY. This connector delivers at least once, so \
                 a crash replays transactions Feldera has already applied. Without a key Feldera \
                 counts row occurrences instead of replacing them: a replayed insert leaves the \
                 row at weight 2, one later DELETE only brings it back to 1, and the row \
                 survives forever. Deleting a row that is not present is undefined for a \
                 keyless table and can crash the pipeline.",
                contract.table.name
            );

            if sink.require_primary_key {
                return Err(explanation.into());
            }

            eprintln!("WARNING: {explanation}");
            eprintln!(
                "WARNING: FELDERA_REQUIRE_PRIMARY_KEY=false, so this runs anyway. The \
                 divergence above is now reachable."
            );
        } else {
            println!(
                "Feldera identifies rows in {} by ({}); sourced from {}",
                contract.table.name,
                identifier_list(&contract.primary_key),
                source
            );
        }

        routes.push(TableRoute {
            ingress_url: sink.ingress_url(&feldera),
            source,
            contract,
            session_keys: Mutex::new(BTreeSet::new()),
        });
    }

    // --------------------------------------------------------
    // PostgreSQL
    // --------------------------------------------------------

    let base_connection = env_or(
        "POSTGRES_URL",
        "host=localhost user=postgres password=postgres",
    );

    // Asked once, before the stream opens. Re-reading it per session would
    // make every reconnection depend on a second successful connection, which
    // is a worse trade than running on a stale value that only changes when an
    // administrator changes it.
    let status_interval = negotiate_status_interval(&base_connection).await?;

    let snapshot_mode = SnapshotMode::from_env()?;

    let give_up = Duration::from_secs(
        env_or("RECONNECT_GIVE_UP_SECS", RECONNECT_GIVE_UP_DEFAULT_SECS).parse::<u64>()?,
    );

    let connector = Connector {
        // `replication=database` is appended rather than left to the caller
        // because without it the connection is an ordinary one and
        // START_REPLICATION fails much later with a confusing syntax error.
        replication_connection: format!("{base_connection} replication=database"),
        base_connection,
        slot: env_or("PG_SLOT", "rust_slot"),
        publication: env_or("PG_PUBLICATION", "my_pub"),
        status_interval,
        snapshot_mode,
        snapshot_state_path: PathBuf::from(env_or("SNAPSHOT_STATE_PATH", "./cdc-state.json")),
        snapshot_batch_rows: env_or("SNAPSHOT_BATCH_ROWS", SNAPSHOT_BATCH_ROWS_DEFAULT)
            .parse::<usize>()?
            .max(1),
        // Clamped to at least one because zero would mean flushing a batch that
        // carries nothing, forever, without ever draining the outbox.
        batch_max_rows: env_or("BATCH_MAX_ROWS", BATCH_MAX_ROWS_DEFAULT)
            .parse::<usize>()?
            .max(1),
        batch_linger: Duration::from_millis(
            env_or("BATCH_LINGER_MS", BATCH_LINGER_MS_DEFAULT).parse::<u64>()?,
        ),
        give_up,
        sink,
        routes,
        crash,
    };

    // --------------------------------------------------------
    // Supervision
    // --------------------------------------------------------
    //
    // Installed before the first connection so that a signal arriving during
    // startup is caught rather than killing the process by default action.

    let mut shutdown = Shutdown::install()?;

    // --------------------------------------------------------
    // Initial snapshot
    // --------------------------------------------------------
    //
    // Ahead of the loop rather than inside it because it is not part of
    // streaming and cannot be retried the way a session can: a snapshot has no
    // resumable position, so the unit that gets retried is the whole thing, and
    // that retry is a process restart. Its own internal retry covers a Feldera
    // that stumbles; anything worse exits nonzero with the state file saying
    // `pending`, which is exactly the instruction the next run needs.

    if let Progress::Stopped(signal) = connector.prepare(&mut shutdown).await? {
        println!(
            "Stopped on {signal} before the snapshot finished; the next run will read the table \
             again from a new consistent point"
        );

        return Ok(());
    }

    // Held out here rather than inside a session because the position and the
    // crash counter describe the connector's progress, which a dropped
    // connection does not undo.
    let mut position = Position::default();
    let mut batches: u64 = 0;

    let mut backoff = RECONNECT_MIN;
    let mut failing_since: Option<Instant> = None;

    loop {
        let flush_before = position.flush;

        let ended = connector
            .run_session(&mut shutdown, &mut position, &mut batches)
            .await?;

        match ended {
            SessionEnd::Shutdown(signal) => {
                println!("Stopped on {signal}");

                return Ok(());
            }

            SessionEnd::Disconnected(reason) => {
                // Progress resets the backoff, not merely a connection that
                // came up. A session that reconnects, replays, fails at the
                // same transaction and disconnects again has achieved nothing,
                // and counting that as recovery would disarm the give-up
                // deadline exactly when it is the only thing left working.
                if position.flush > flush_before {
                    backoff = RECONNECT_MIN;
                    failing_since = None;
                }

                let since = *failing_since.get_or_insert_with(Instant::now);

                if since.elapsed() >= give_up {
                    return Err(format!(
                        "no replication progress for {:?}, so giving up rather than holding the \
                         slot open any longer. Last failure: {reason}",
                        since.elapsed()
                    )
                    .into());
                }

                let delay = jittered(backoff);

                eprintln!("Replication session ended ({reason}); reconnecting in {delay:?}");

                // Interruptible on purpose. A signal arriving during a 30
                // second backoff should stop the process, not be queued behind
                // a sleep that exists only to be polite to PostgreSQL.
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}

                    signal = shutdown.requested() => {
                        println!("Stopped on {signal} while waiting to reconnect");

                        return Ok(());
                    }
                }

                backoff = (backoff * 2).min(RECONNECT_MAX);
            }
        }
    }
}
