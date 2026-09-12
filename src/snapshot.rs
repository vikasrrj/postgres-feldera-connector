//! Initial snapshot: exported slot snapshot, state file, catalog.

use std::io::Write;
use std::path::Path;

use tokio_postgres::Client;
use tokio_postgres::types::PgLsn;

use crate::config::env_or;
use crate::contract::{TableName, TableRoute};
use crate::decode::ColumnMetadata;

// ============================================================
// SNAPSHOT
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum SnapshotMode {
    /// Read the table and the stream from one consistent point.
    Initial,

    /// Stream only. Correct when the target already holds the table.
    Never,

    /// Read the table first and create the slot afterwards; loses the window
    /// between the two. Present so the loss can be measured.
    Inconsistent,
}

impl SnapshotMode {
    pub(crate) fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        match env_or("SNAPSHOT_MODE", "initial").as_str() {
            "initial" => Ok(Self::Initial),
            "never" => Ok(Self::Never),
            "inconsistent" => Ok(Self::Inconsistent),

            other => Err(format!(
                "SNAPSHOT_MODE must be \"initial\", \"never\" or \"inconsistent\", got {other:?}"
            )
            .into()),
        }
    }
}

/// How many rows to put in one request to Feldera.
pub(crate) const SNAPSHOT_BATCH_ROWS_DEFAULT: &str = "1000";

/// What a previous run left behind about the snapshot of one slot.
pub(crate) struct SnapshotState {
    /// Which slot this describes.
    pub(crate) slot: String,

    /// The LSN at which the snapshot and the stream meet.
    pub(crate) consistent_point: u64,

    /// False while the table is being read.
    pub(crate) complete: bool,
}

impl SnapshotState {
    pub(crate) fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "slot": self.slot,
            "consistent_point": PgLsn::from(self.consistent_point).to_string(),
            "snapshot": if self.complete { "complete" } else { "pending" },
        })
    }

    pub(crate) fn from_json(value: &serde_json::Value) -> Result<Self, Box<dyn std::error::Error>> {
        let slot = value
            .get("slot")
            .and_then(|slot| slot.as_str())
            .ok_or("state file carries no slot name")?
            .to_string();

        let raw = value
            .get("consistent_point")
            .and_then(|point| point.as_str())
            .ok_or("state file carries no consistent point")?;

        let consistent_point = u64::from(
            raw.parse::<PgLsn>()
                .map_err(|_| format!("state file has an unreadable LSN {raw:?}"))?,
        );

        let complete = match value.get("snapshot").and_then(|stage| stage.as_str()) {
            Some("complete") => true,
            Some("pending") => false,

            other => {
                return Err(format!(
                    "state file snapshot stage must be \"complete\" or \"pending\", got {other:?}"
                )
                .into());
            }
        };

        Ok(Self {
            slot,
            consistent_point,
            complete,
        })
    }
}

/// Read the state file, treating absence as "no previous run".
pub(crate) fn read_snapshot_state(
    path: &Path,
) -> Result<Option<SnapshotState>, Box<dyn std::error::Error>> {
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,

        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),

        Err(error) => {
            return Err(format!("could not read {}: {error}", path.display()).into());
        }
    };

    let value: serde_json::Value = serde_json::from_str(&raw)
        .map_err(|error| format!("could not parse {}: {error}", path.display()))?;

    let state = SnapshotState::from_json(&value)
        .map_err(|error| format!("{} is not usable: {error}", path.display()))?;

    Ok(Some(state))
}

/// Write the state file atomically: temp file, fsync, rename, fsync the dir.
pub(crate) fn write_snapshot_state(
    path: &Path,
    state: &SnapshotState,
) -> Result<(), Box<dyn std::error::Error>> {
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());

    if let Some(directory) = directory {
        std::fs::create_dir_all(directory)?;
    }

    let temporary = path.with_extension("json.tmp");

    {
        let mut file = std::fs::File::create(&temporary)?;

        file.write_all(state.to_json().to_string().as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }

    std::fs::rename(&temporary, path)?;

    if let Some(directory) = directory {
        std::fs::File::open(directory)?.sync_all()?;
    }

    Ok(())
}

pub(crate) async fn slot_exists(
    client: &Client,
    slot: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let row = client
        .query_opt(
            "select confirmed_flush_lsn::text from pg_replication_slots where slot_name = $1",
            &[&slot],
        )
        .await?;

    Ok(row.is_some())
}

// TRUNCATE is a publication option, not a table one, so a publication without
// it silently leaves Feldera holding rows the source erased.
pub(crate) async fn warn_if_publication_skips_truncate(
    client: &Client,
    publication: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let row = client
        .query_opt(
            "select pubtruncate from pg_publication where pubname = $1",
            &[&publication],
        )
        .await?;

    match row {
        None => Err(format!(
            "PostgreSQL has no publication {publication:?}. Create it, or set PG_PUBLICATION \
             to the one this connector should read."
        )
        .into()),

        Some(row) if row.try_get::<_, bool>(0)? => Ok(()),

        Some(_) => {
            eprintln!(
                "WARNING: publication {publication:?} has pubtruncate = false, so TRUNCATE on \
                 the source will not appear on the stream and Feldera will keep every row. \
                 ALTER PUBLICATION {publication} SET (publish = 'insert, update, delete, truncate');"
            );

            Ok(())
        }
    }
}

// A table in TABLES that the publication does not list will be snapshotted and
// then never updated; a published table that TABLES does not name will stop the
// session at its first RELATION.
pub(crate) async fn check_publication_tables(
    client: &Client,
    publication: &str,
    routes: &[TableRoute],
) -> Result<(), Box<dyn std::error::Error>> {
    let rows = client
        .query(
            "select schemaname, tablename from pg_publication_tables where pubname = $1",
            &[&publication],
        )
        .await?;

    let published: Vec<(String, String)> = rows
        .iter()
        .map(|row| Ok((row.try_get::<_, String>(0)?, row.try_get::<_, String>(1)?)))
        .collect::<Result<_, tokio_postgres::Error>>()?;

    for route in routes {
        if !published
            .iter()
            .any(|(schema, table)| route.matches_source(schema, table))
        {
            return Err(format!(
                "{} is in TABLES but not in publication {publication:?}. The snapshot would load \
                 it and the stream would never update it. ALTER PUBLICATION {publication} ADD \
                 TABLE {};",
                route.source, route.source
            )
            .into());
        }
    }

    for (schema, table) in &published {
        if !routes
            .iter()
            .any(|route| route.matches_source(schema, table))
        {
            eprintln!(
                "WARNING: publication {publication:?} also contains {schema}.{table}, which is \
                 not in TABLES. The first RELATION for it will stop the connector. Add it to \
                 TABLES, or drop it from the publication."
            );
        }
    }

    Ok(())
}

// Read the table's columns in the order COPY will emit them. Drawn from the
// catalog rather than a RELATION message, because the snapshot runs before any
// replication starts. attnum > 0 drops system columns and attisdropped drops
// the tombstones a dropped column leaves behind.
pub(crate) async fn snapshot_column_metadata(
    client: &Client,
    table: &TableName,
) -> Result<Vec<ColumnMetadata>, Box<dyn std::error::Error>> {
    let rows = client
        .query(
            "select a.attname, \
                    a.atttypid::int4, \
                    (c.relreplident = 'f' or exists ( \
                       select 1 from pg_index i \
                       where i.indrelid = c.oid \
                         and (i.indisreplident \
                              or (c.relreplident = 'd' and i.indisprimary)) \
                         and a.attnum = any (i.indkey) \
                     )) as is_key \
             from pg_attribute a \
             join pg_class c on c.oid = a.attrelid \
             join pg_namespace n on n.oid = c.relnamespace \
             where n.nspname = $1 \
               and c.relname = $2 \
               and a.attnum > 0 \
               and not a.attisdropped \
             order by a.attnum",
            &[&table.schema, &table.table],
        )
        .await?;

    if rows.is_empty() {
        return Err(format!(
            "PostgreSQL has no table {table}, so there is nothing to snapshot. Set TABLES (or \
             PG_TABLE) to a table that exists."
        )
        .into());
    }

    let mut columns = Vec::with_capacity(rows.len());

    for row in &rows {
        columns.push(ColumnMetadata {
            name: row.try_get::<_, String>(0)?,
            type_id: row.try_get::<_, i32>(1)?,
            is_key: row.try_get::<_, bool>(2)?,
        });
    }

    Ok(columns)
}

/// A slot just created by `CREATE_REPLICATION_SLOT`.
pub(crate) struct CreatedSlot {
    /// The LSN the slot starts at; START_REPLICATION 0/0 resumes here.
    pub(crate) consistent_point: u64,

    /// The exported snapshot, if one was asked for. Valid only while the
    /// connection that created it stays open.
    pub(crate) snapshot_name: Option<String>,
}

/// The result of a step a shutdown signal is allowed to cut short.
pub(crate) enum Progress<T> {
    Done(T),
    Stopped(&'static str),
}

/// What the connector should do before it starts streaming.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum StartupPlan {
    /// The slot holds a position this connector put there, and the target
    /// already holds everything up to it.
    Resume,

    /// Create the slot and stream from now, leaving the pre-existing table
    /// contents out of the target.
    StreamWithoutSnapshot,

    /// Read the table and the stream from one consistent point.
    Snapshot { drop_existing_slot: bool },
}

// Decide what to do from the two facts available at startup: what the state
// file says, and whether the slot is there.
pub(crate) fn plan_startup(
    mode: SnapshotMode,
    slot: &str,
    state: Option<&SnapshotState>,
    slot_exists: bool,
) -> StartupPlan {
    if mode == SnapshotMode::Never {
        // Reads and writes no state on purpose.
        if slot_exists {
            return StartupPlan::Resume;
        }

        eprintln!(
            "WARNING: SNAPSHOT_MODE=never and slot {slot:?} does not exist, so it will be created \
             and streaming will start from now. Any row already in the table is invisible to this \
             connector: the WAL that inserted it has been recycled or was never captured by a \
             slot. If the target does not already hold those rows, it never will."
        );

        return StartupPlan::StreamWithoutSnapshot;
    }

    let Some(state) = state.filter(|state| state.slot == slot) else {
        if slot_exists {
            // A slot proves a position; it says nothing about whether the
            // target was ever seeded, so "no record" is read as "not done".
            eprintln!(
                "WARNING: slot {slot:?} exists but no snapshot is recorded for it. The slot's \
                 position says nothing about whether the target was ever seeded, so the slot \
                 will be dropped and replaced by a fresh snapshot. Set SNAPSHOT_MODE=never to \
                 stream from the existing slot instead, which is correct only if the target \
                 already holds the table."
            );

            return StartupPlan::Snapshot {
                drop_existing_slot: true,
            };
        }

        return StartupPlan::Snapshot {
            drop_existing_slot: false,
        };
    };

    match (state.complete, slot_exists) {
        (true, true) => StartupPlan::Resume,

        (true, false) => {
            // The slot was the only thing retaining the WAL written since the
            // last acknowledgement, and it is gone.
            eprintln!(
                "WARNING: the snapshot recorded for slot {slot:?} is complete, but the slot no \
                 longer exists. Everything written since the last acknowledgement is \
                 unrecoverable, so the table will be read again from a new slot."
            );

            StartupPlan::Snapshot {
                drop_existing_slot: false,
            }
        }

        // A snapshot is a replacement, not a delta; the retract stands in for
        // the delete whose WAL was never captured.
        (false, slot_exists) => {
            println!(
                "A previous snapshot of slot {slot:?} did not finish; taking a fresh one from a \
                 new consistent point"
            );

            StartupPlan::Snapshot {
                drop_existing_slot: slot_exists,
            }
        }
    }
}
