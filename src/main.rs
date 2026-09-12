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

/// Turn a base connection string into a replication connection string without
/// corrupting it: `?param=value` is only valid in the `postgresql://` URL form,
/// conninfo strings (`host=.. password=..`) are whitespace-separated.
fn as_replication_connection(base: &str) -> String {
    let base = base.trim();

    if base.starts_with("postgresql://") || base.starts_with("postgres://") {
        if base.contains('?') {
            format!("{base}&replication=database")
        } else {
            format!("{base}?replication=database")
        }
    } else {
        format!("{base} replication=database")
    }
}

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

    let status_interval = negotiate_status_interval(&base_connection).await?;

    let snapshot_mode = SnapshotMode::from_env()?;

    let give_up = Duration::from_secs(
        env_or("RECONNECT_GIVE_UP_SECS", RECONNECT_GIVE_UP_DEFAULT_SECS).parse::<u64>()?,
    );

    let connector = Connector {
        // replication=database: without it START_REPLICATION fails later with a confusing error.
        // The separator differs by connection-string form: URL strings take a query `?`,
        // conninfo strings take a whitespace-delimited option.
        replication_connection: as_replication_connection(&base_connection),
        base_connection,
        slot: env_or("PG_SLOT", "rust_slot"),
        publication: env_or("PG_PUBLICATION", "my_pub"),
        status_interval,
        snapshot_mode,
        snapshot_state_path: PathBuf::from(env_or("SNAPSHOT_STATE_PATH", "./cdc-state.json")),
        snapshot_batch_rows: env_or("SNAPSHOT_BATCH_ROWS", SNAPSHOT_BATCH_ROWS_DEFAULT)
            .parse::<usize>()?
            .max(1),
        // Clamped to at least 1 so a flush never carries nothing.
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

    let mut shutdown = Shutdown::install()?;

    // --------------------------------------------------------
    // Initial snapshot
    // --------------------------------------------------------

    if let Progress::Stopped(signal) = connector.prepare(&mut shutdown).await? {
        println!(
            "Stopped on {signal} before the snapshot finished; the next run will read the table \
             again from a new consistent point"
        );

        return Ok(());
    }

    // Position and crash counter outlive the session loop below.
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
                // Only real progress resets the backoff.
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

                // Interruptible so a signal is not queued behind the sleep.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conninfo_form_gets_a_space_separated_option() {
        assert_eq!(
            as_replication_connection("host=localhost user=postgres password=postgres"),
            "host=localhost user=postgres password=postgres replication=database"
        );
    }

    #[test]
    fn conninfo_password_is_not_corrupted() {
        let connection = as_replication_connection("host=h port=1 user=u password=p");

        assert!(connection.contains("password=p"));
        assert!(connection.contains("password=p "));
        assert!(connection.contains("replication=database"));
    }

    #[test]
    fn url_form_gets_a_query_option() {
        assert_eq!(
            as_replication_connection("postgresql://u:p@localhost/db"),
            "postgresql://u:p@localhost/db?replication=database"
        );
    }

    #[test]
    fn url_form_with_existing_query_appends() {
        assert_eq!(
            as_replication_connection("postgresql://u:p@localhost/db?sslmode=require"),
            "postgresql://u:p@localhost/db?sslmode=require&replication=database"
        );
    }
}
