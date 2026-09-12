//! The replication session: snapshot, stream, flush, retract.

use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::pin;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use postgres_replication::LogicalReplicationStream;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, SimpleQueryMessage};

use crate::contract::{
    TableRoute, quote_literal, refuse_unusable_identity, report_identity_coverage, route_index_for,
    truncate_route_indices,
};
use crate::decode::{
    ColumnMetadata, RelationMetadata, decode_copy_row, decode_previous_row, decode_row,
    fill_toast_holes, quote_ident, row_to_json,
};
use crate::feldera::{Barrier, CdcEvent, FelderaSink, SLOW_WAIT_WARNING, SinkError};
use crate::outbox::{Outbox, Transaction, buffer_change};
use crate::runtime::{
    CrashConfig, CrashPoint, Position, RECONNECT_MAX, RECONNECT_MIN, SHUTDOWN_DEADLINE, SessionEnd,
    Shutdown, WaitOutcome, confirm_slot_recorded, connect_pg, jittered, maybe_crash,
    send_standby_status,
};
use crate::snapshot::{
    CreatedSlot, Progress, SnapshotMode, SnapshotState, StartupPlan, check_publication_tables,
    plan_startup, read_snapshot_state, slot_exists, snapshot_column_metadata,
    warn_if_publication_skips_truncate, write_snapshot_state,
};

// ============================================================
// SESSION
// ============================================================

/// Everything that outlives a single replication connection.
///
/// Split out of `main` because a reconnect has to be able to start a fresh
/// session without redoing startup. The Feldera contract is checked once, on
/// the way in; re-reading it on every hiccup would let a stumble in Feldera's
/// control plane block recovery of its data plane.
pub(crate) struct Connector {
    pub(crate) replication_connection: String,
    pub(crate) base_connection: String,
    pub(crate) slot: String,
    pub(crate) publication: String,
    pub(crate) status_interval: Duration,
    pub(crate) snapshot_mode: SnapshotMode,
    pub(crate) snapshot_state_path: PathBuf,
    pub(crate) snapshot_batch_rows: usize,
    pub(crate) batch_max_rows: usize,
    pub(crate) batch_linger: Duration,
    pub(crate) give_up: Duration,
    pub(crate) sink: FelderaSink,
    pub(crate) routes: Vec<TableRoute>,
    pub(crate) crash: Option<CrashConfig>,
}

impl Connector {
    /// Bring the target up to a state the stream can carry forward from.
    ///
    /// Runs once, before any streaming, and is a no-op on every restart after
    /// the first. Returning `Ready` is a claim with content: the target holds
    /// every row that existed at the slot's consistent point, and the slot is
    /// positioned so that the stream delivers everything committed after it.
    pub(crate) async fn prepare(
        &self,
        shutdown: &mut Shutdown,
    ) -> Result<Progress<()>, Box<dyn std::error::Error>> {
        // `never` reads no state, so that an unreadable file left behind by a
        // differently configured run cannot fail a mode that would not have
        // consulted it anyway.
        let state = match self.snapshot_mode {
            SnapshotMode::Never => None,
            _ => read_snapshot_state(&self.snapshot_state_path)?,
        };

        let (client, driver) = connect_pg(&self.base_connection).await?;

        let exists = slot_exists(&client, &self.slot).await?;

        warn_if_publication_skips_truncate(&client, &self.publication).await?;

        check_publication_tables(&client, &self.publication, &self.routes).await?;

        drop(client);
        let _ = driver.await;

        let plan = plan_startup(self.snapshot_mode, &self.slot, state.as_ref(), exists);

        match plan {
            StartupPlan::Resume => {
                println!(
                    "Slot {:?} already carries a snapshotted position; streaming from it",
                    self.slot
                );

                Ok(Progress::Done(()))
            }

            StartupPlan::StreamWithoutSnapshot => {
                let (client, driver, created) = self.create_slot(false).await?;

                drop(client);
                let _ = driver.await;

                println!(
                    "Created slot {:?} at {}; streaming from there without reading the table",
                    self.slot,
                    PgLsn::from(created.consistent_point)
                );

                Ok(Progress::Done(()))
            }

            StartupPlan::Snapshot { drop_existing_slot } => {
                self.take_snapshot(shutdown, drop_existing_slot).await
            }
        }
    }

    /// Create the replication slot, optionally exporting a snapshot, and hand
    /// back the connection that owns it.
    ///
    /// The connection is returned rather than closed because an exported
    /// snapshot only exists for as long as the replication connection that
    /// created it stays open and issues no further command. Closing here and
    /// reconnecting to read the table would be the same bug the whole phase is
    /// about, just better hidden: the snapshot name would still be returned and
    /// `SET TRANSACTION SNAPSHOT` would fail against a snapshot that no longer
    /// exists.
    pub(crate) async fn create_slot(
        &self,
        export_snapshot: bool,
    ) -> Result<(Client, tokio::task::JoinHandle<()>, CreatedSlot), Box<dyn std::error::Error>>
    {
        let (client, driver) = connect_pg(&self.replication_connection).await?;

        // The option is always written out, because `export` is the *default*
        // and exporting a snapshot nobody reads is not free: the walsender
        // holds the snapshot so other sessions can import it, which pins its
        // xmin and stops vacuum from cleaning up anything newer for as long as
        // the connection lives. Measured on 16.15 — `export` leaves a
        // walsender with a non-null `backend_xmin` and `nothing` does not.
        //
        // This parenthesised option list is the form documented since
        // PostgreSQL 15; the older `EXPORT_SNAPSHOT` keyword still parses, but
        // only for backward compatibility.
        let option = if export_snapshot { "export" } else { "nothing" };

        let command = format!(
            "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput (SNAPSHOT '{option}')",
            quote_ident(&self.slot)
        );

        let messages = client.simple_query(&command).await?;

        let row = messages
            .iter()
            .find_map(|message| match message {
                SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .ok_or_else(|| {
                format!(
                    "CREATE_REPLICATION_SLOT for {:?} returned no row, so neither its position \
                     nor its snapshot can be known",
                    self.slot
                )
            })?;

        let raw = row
            .try_get("consistent_point")?
            .ok_or("CREATE_REPLICATION_SLOT reported no consistent point")?;

        let consistent_point = u64::from(
            raw.parse::<PgLsn>()
                .map_err(|_| format!("could not parse consistent point {raw:?}"))?,
        );

        let snapshot_name = row
            .try_get("snapshot_name")?
            .filter(|name| !name.is_empty())
            .map(|name| name.to_string());

        Ok((
            client,
            driver,
            CreatedSlot {
                consistent_point,
                snapshot_name,
            },
        ))
    }

    pub(crate) async fn take_snapshot(
        &self,
        shutdown: &mut Shutdown,
        drop_existing_slot: bool,
    ) -> Result<Progress<()>, Box<dyn std::error::Error>> {
        if drop_existing_slot {
            let (client, driver) = connect_pg(&self.base_connection).await?;

            client
                .execute("select pg_drop_replication_slot($1)", &[&self.slot])
                .await?;

            drop(client);
            let _ = driver.await;

            println!("Dropped slot {:?}", self.slot);
        }

        match self.snapshot_mode {
            SnapshotMode::Initial => self.snapshot_from_exported_point(shutdown).await,

            SnapshotMode::Inconsistent => self.snapshot_before_the_slot(shutdown).await,

            // `plan_startup` answers `StreamWithoutSnapshot` for this mode, so
            // arriving here means the plan and this match disagree.
            SnapshotMode::Never => {
                Err("SNAPSHOT_MODE=never asked for a snapshot, which is a bug".into())
            }
        }
    }

    /// Read the table as it stood at the slot's consistent point.
    ///
    /// The ordering is the entire content of this phase. The slot is created
    /// first and exports the snapshot it started from, the table is read inside
    /// that snapshot, and the stream then resumes at exactly that LSN. Every
    /// row of the table is therefore delivered by exactly one of the two paths:
    /// if it was committed at or before the consistent point the COPY sees it
    /// and the stream does not replay it, and if it was committed after, the
    /// COPY's snapshot cannot see it and the stream carries it. There is no
    /// window in which a row belongs to neither.
    pub(crate) async fn snapshot_from_exported_point(
        &self,
        shutdown: &mut Shutdown,
    ) -> Result<Progress<()>, Box<dyn std::error::Error>> {
        let (replication_client, replication_driver, created) = self.create_slot(true).await?;

        let Some(snapshot_name) = created.snapshot_name.clone() else {
            return Err("CREATE_REPLICATION_SLOT exported no snapshot to read the table in".into());
        };

        println!(
            "Slot {:?} created at {} with exported snapshot {}",
            self.slot,
            PgLsn::from(created.consistent_point),
            snapshot_name
        );

        // Written before a single row moves, and that order is deliberate. A
        // crash from here on leaves a file saying `pending`, which is what
        // makes the next run read the table again instead of streaming forward
        // from a target holding some unknown fraction of it.
        write_snapshot_state(
            &self.snapshot_state_path,
            &SnapshotState {
                slot: self.slot.clone(),
                consistent_point: created.consistent_point,
                complete: false,
            },
        )?;

        let (reader, reader_driver) = connect_pg(&self.base_connection).await?;

        // REPEATABLE READ is required, not merely chosen. `SET TRANSACTION
        // SNAPSHOT` needs a transaction whose snapshot is taken once and reused,
        // and READ COMMITTED takes a fresh one per statement, which is exactly
        // the property being overridden here.
        reader
            .batch_execute("begin isolation level repeatable read")
            .await?;

        reader
            .batch_execute(&format!(
                "set transaction snapshot {}",
                quote_literal(&snapshot_name)
            ))
            .await?;

        let outcome = self.copy_table_into_sink(&reader, shutdown).await?;

        // Ending the transaction before the connection closes is tidiness, not
        // correctness — a read-only REPEATABLE READ transaction commits and
        // rolls back identically — but leaving it open would hold a snapshot
        // and with it the oldest xmin the server has to keep, which is how a
        // forgotten transaction blocks vacuum.
        reader.batch_execute("commit").await?;

        drop(reader);
        let _ = reader_driver.await;

        // Only now may the replication connection go. It owned the exported
        // snapshot, and the COPY above is the thing that needed it alive.
        drop(replication_client);
        let _ = replication_driver.await;

        let rows = match outcome {
            Progress::Done(rows) => rows,

            // The state file is left saying `pending`, which is the honest
            // record: the target holds part of a snapshot and the next run has
            // to read the table again.
            Progress::Stopped(signal) => return Ok(Progress::Stopped(signal)),
        };

        write_snapshot_state(
            &self.snapshot_state_path,
            &SnapshotState {
                slot: self.slot.clone(),
                consistent_point: created.consistent_point,
                complete: true,
            },
        )?;

        println!(
            "Snapshot of {} complete: {rows} row(s); the stream picks up at {}",
            self.route_names(),
            PgLsn::from(created.consistent_point)
        );

        Ok(Progress::Done(()))
    }

    /// Read the table first and create the slot afterwards.
    ///
    /// This is what the code looks like when the handoff is not thought about,
    /// and it loses data rather than duplicating it. The COPY sees the table at
    /// some time T, the slot's consistent point lands at some later time C, and
    /// every row committed in between is in neither: too late for the snapshot,
    /// too early for the stream. Nothing downstream can detect it, because the
    /// rows simply never appear and no error is raised on either side.
    ///
    /// Note that the opposite mistake is harmless, which is why the ordering has
    /// to be reasoned about rather than guessed. A snapshot taken *after* the
    /// consistent point overlaps the stream instead of leaving a hole, and the
    /// primary key absorbs the overlap.
    pub(crate) async fn snapshot_before_the_slot(
        &self,
        shutdown: &mut Shutdown,
    ) -> Result<Progress<()>, Box<dyn std::error::Error>> {
        eprintln!(
            "WARNING: SNAPSHOT_MODE=inconsistent. The table is read before the replication slot \
             exists, so every row committed between the read and the slot's consistent point is \
             delivered by neither path and is lost permanently and silently. This mode exists to \
             demonstrate that loss, not to be run for real."
        );

        let (reader, reader_driver) = connect_pg(&self.base_connection).await?;

        reader
            .batch_execute("begin isolation level repeatable read")
            .await?;

        let outcome = self.copy_table_into_sink(&reader, shutdown).await?;

        reader.batch_execute("commit").await?;

        drop(reader);
        let _ = reader_driver.await;

        let rows = match outcome {
            Progress::Done(rows) => rows,
            Progress::Stopped(signal) => return Ok(Progress::Stopped(signal)),
        };

        let (client, driver, created) = self.create_slot(false).await?;

        drop(client);
        let _ = driver.await;

        // No `pending` state is recorded on the way in, because until the slot
        // exists there is no consistent point to record — which is another way
        // of saying this ordering has no point at which the snapshot and the
        // stream are known to meet.
        write_snapshot_state(
            &self.snapshot_state_path,
            &SnapshotState {
                slot: self.slot.clone(),
                consistent_point: created.consistent_point,
                complete: true,
            },
        )?;

        eprintln!(
            "WARNING: read {rows} row(s) of {}, then created slot {:?} at {}. Anything committed \
             between those two moments has been lost.",
            self.route_names(),
            self.slot,
            PgLsn::from(created.consistent_point)
        );

        Ok(Progress::Done(()))
    }

    pub(crate) fn route_names(&self) -> String {
        self.routes
            .iter()
            .map(|route| route.source.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Stream every configured table out of PostgreSQL and into Feldera.
    ///
    /// Retract all, then COPY all. The exported snapshot is one consistent
    /// picture of every table, and a replacement that empties `users` and then
    /// reloads it before touching `items` leaves the two tables describing
    /// different moments for the length of the second COPY.
    pub(crate) async fn copy_table_into_sink(
        &self,
        reader: &Client,
        shutdown: &mut Shutdown,
    ) -> Result<Progress<u64>, Box<dyn std::error::Error>> {
        for route in &self.routes {
            if let Progress::Stopped(signal) = self.retract_live_target(route, shutdown).await? {
                return Ok(Progress::Stopped(signal));
            }
        }

        let mut total = 0_u64;

        for route in &self.routes {
            match self.copy_one_table(reader, route, shutdown).await? {
                Progress::Done(rows) => total += rows,
                Progress::Stopped(signal) => return Ok(Progress::Stopped(signal)),
            }
        }

        Ok(Progress::Done(total))
    }

    /// Stream one table out of PostgreSQL and into Feldera in batches.
    ///
    /// `COPY ... TO STDOUT` rather than `SELECT *` because it is the one way to
    /// read a table without materialising it: the server writes rows as it
    /// finds them and the client decides when to read more, so memory here is
    /// one batch rather than one table. A `SELECT` through this driver would
    /// buffer the whole result before the first row could be decoded.
    pub(crate) async fn copy_one_table(
        &self,
        reader: &Client,
        route: &TableRoute,
        shutdown: &mut Shutdown,
    ) -> Result<Progress<u64>, Box<dyn std::error::Error>> {
        let columns = snapshot_column_metadata(reader, &route.source).await?;

        refuse_unusable_identity(
            &route.contract,
            &route.source.schema,
            &route.source.table,
            &columns,
        )?;

        // Listing the columns explicitly rather than letting `COPY` default to
        // all of them makes the order of the output the order of the catalog
        // query, instead of two independent things that happen to agree today.
        let column_list = columns
            .iter()
            .map(|column| quote_ident(&column.name))
            .collect::<Vec<_>>()
            .join(", ");

        println!(
            "Reading {} ({}) at the snapshot",
            route.source,
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );

        let copy = reader
            .copy_out_simple(&format!(
                "copy {} ({column_list}) to stdout",
                route.source.quoted()
            ))
            .await?;

        let mut copy = pin!(copy);

        // The COPY arrives as byte chunks that have nothing to do with row
        // boundaries, so a partial row at the end of one chunk has to be kept
        // until the rest of it turns up. Splitting on a raw newline is safe
        // because `COPY`'s text format escapes any newline inside a value.
        let mut pending = Vec::new();

        let mut batch = Vec::new();
        let mut rows: u64 = 0;
        let mut batches: u64 = 0;

        let started = Instant::now();

        while let Some(chunk) = copy.next().await {
            pending.extend_from_slice(&chunk?);

            let mut consumed = 0;

            while let Some(offset) = pending[consumed..].iter().position(|byte| *byte == b'\n') {
                let end = consumed + offset;

                let row = decode_copy_row(&columns, &pending[consumed..end])?;

                consumed = end + 1;
                rows += 1;

                batch.push(serde_json::json!({ "insert": row_to_json(&row)? }));

                if batch.len() >= self.snapshot_batch_rows {
                    batches += 1;

                    if let Progress::Stopped(signal) =
                        self.deliver_batch(route, &batch, batches, shutdown).await?
                    {
                        return Ok(Progress::Stopped(signal));
                    }

                    batch.clear();
                }
            }

            pending.drain(..consumed);
        }

        // `COPY` terminates every row with a newline, so anything left is a row
        // the server never finished sending. Decoding it would fabricate a row
        // out of a truncated one.
        if !pending.is_empty() {
            return Err(format!(
                "COPY of {} ended in the middle of a row after {rows} complete row(s)",
                route.source
            )
            .into());
        }

        if !batch.is_empty() {
            batches += 1;

            if let Progress::Stopped(signal) =
                self.deliver_batch(route, &batch, batches, shutdown).await?
            {
                return Ok(Progress::Stopped(signal));
            }
        }

        println!(
            "  Read and delivered {rows} row(s) in {batches} batch(es) in {:?}",
            started.elapsed()
        );

        Ok(Progress::Done(rows))
    }

    /// Rows to retract: Feldera's live set, unioned with keys this process
    /// already made durable for this table.
    pub(crate) async fn retract_events(
        &self,
        route: &TableRoute,
    ) -> Result<Vec<serde_json::Value>, SinkError> {
        match self.sink.live_delete_events(&route.contract).await {
            Ok(queried) => Ok(route.merge_retract_events(queried)),

            Err(error) => {
                let fallback = route.session_delete_events();

                if fallback.is_empty() {
                    return Err(error);
                }

                eprintln!(
                    "WARNING: could not query Feldera's live rows for {} ({error}); retracting \
                     {} key(s) this process made durable",
                    route.contract.table.name,
                    fallback.len()
                );

                Ok(fallback)
            }
        }
    }

    /// Retract every row Feldera currently holds for this table.
    ///
    /// Used by the snapshot, which is a replacement, and by TRUNCATE, which is
    /// the same retract without a COPY after it. The rows arrive already shaped
    /// as delete events, and they go out through the same `deliver_batch` path
    /// as snapshot inserts so a crash mid-retract leaves `pending` in the
    /// state file and the next run starts the replacement again.
    pub(crate) async fn retract_live_target(
        &self,
        route: &TableRoute,
        shutdown: &mut Shutdown,
    ) -> Result<Progress<()>, Box<dyn std::error::Error>> {
        let deletes = match self.retract_events(route).await {
            Ok(deletes) => deletes,

            Err(SinkError::Unavailable(reason)) => {
                return Err(format!("could not read Feldera's live rows: {reason}").into());
            }

            Err(rejected) => return Err(rejected.into()),
        };

        if deletes.is_empty() {
            println!(
                "  Feldera holds no rows to retract in {}",
                route.contract.table.name
            );

            return Ok(Progress::Done(()));
        }

        println!(
            "  Retracting {} row(s) Feldera still holds in {}",
            deletes.len(),
            route.contract.table.name
        );

        let mut batches = 0_u64;

        for chunk in deletes.chunks(self.snapshot_batch_rows) {
            batches += 1;

            if let Progress::Stopped(signal) =
                self.deliver_batch(route, chunk, batches, shutdown).await?
            {
                return Ok(Progress::Stopped(signal));
            }
        }

        Ok(Progress::Done(()))
    }

    /// Deliver one snapshot batch, retrying while the failure is one a retry
    /// could fix.
    ///
    /// The retry sits at the batch rather than at the process because a snapshot
    /// cannot be resumed from the middle. The exported snapshot dies with the
    /// connection that created it, so there is no way to re-open the same view
    /// of the table and continue from row N — the only alternative to retrying
    /// here is reading the whole table again. It is bounded by the same give-up
    /// deadline as a reconnect, and for the same reason: while this blocks, the
    /// slot is pinning WAL on the source.
    pub(crate) async fn deliver_batch(
        &self,
        route: &TableRoute,
        batch: &[serde_json::Value],
        number: u64,
        shutdown: &mut Shutdown,
    ) -> Result<Progress<()>, Box<dyn std::error::Error>> {
        let started = Instant::now();
        let mut backoff = RECONNECT_MIN;

        loop {
            // Abandoning a send costs nothing that has to be cleaned up: the
            // state file still says `pending`, so a restart reads the table
            // again rather than trusting a partial target.
            let attempt = tokio::select! {
                attempt = async {
                    let barrier = self.sink.send(&route.ingress_url, batch).await?;

                    self.sink.wait(barrier).await
                } => attempt,

                signal = shutdown.requested() => {
                    println!("{signal} received during the snapshot; stopping");

                    return Ok(Progress::Stopped(signal));
                }
            };

            match attempt {
                Ok(_) => {
                    maybe_crash(&self.crash, CrashPoint::DuringSnapshot, number);

                    route.remember_durable_events(batch);

                    return Ok(Progress::Done(()));
                }

                // Identical bytes in front of an endpoint that already refused
                // them. Retrying reads the table again to the same end.
                Err(rejected @ SinkError::Rejected(_)) => return Err(rejected.into()),

                Err(unavailable) => {
                    if started.elapsed() >= self.give_up {
                        return Err(format!(
                            "snapshot batch {number} still undeliverable after {:?}: {unavailable}",
                            started.elapsed()
                        )
                        .into());
                    }

                    let delay = jittered(backoff);

                    eprintln!(
                        "Snapshot batch {number} failed ({unavailable}); retrying in {delay:?}"
                    );

                    tokio::time::sleep(delay).await;

                    backoff = (backoff * 2).min(RECONNECT_MAX);
                }
            }
        }
    }

    /// Wait for a batch to become durable without going deaf while it happens.
    ///
    /// The wait is pinned rather than awaited directly so the status ticker
    /// keeps firing underneath it. Awaiting it plainly is how the two timeouts
    /// collide: the connector stops reading the stream and stops reporting its
    /// position, and PostgreSQL kills a walsender it has not heard from within
    /// `wal_sender_timeout`. Pinning is what lets the same future survive being
    /// polled by several `select!` iterations instead of being restarted from
    /// the beginning by each one.
    pub(crate) async fn wait_for_durability(
        &self,
        barrier: Barrier,
        stream: &mut std::pin::Pin<&mut LogicalReplicationStream>,
        ticker: &mut tokio::time::Interval,
        position: &Position,
        shutdown: &mut Shutdown,
    ) -> Result<WaitOutcome, Box<dyn std::error::Error>> {
        let wait = self.sink.wait(barrier);
        tokio::pin!(wait);

        let outcome = loop {
            tokio::select! {
                result = &mut wait => break result,

                _ = ticker.tick() => {
                    // The *old* flush position. Sending the one this batch is
                    // about to reach would claim a durability the wait has not
                    // established, and a keepalive that lies is worse than no
                    // keepalive at all.
                    if let Err(error) =
                        send_standby_status(stream, position.received, position.flush, 0).await
                    {
                        return Ok(WaitOutcome::Disconnected(format!(
                            "standby status update failed: {error}"
                        )));
                    }

                    println!(
                        "  Sent periodic standby status update while waiting for Feldera"
                    );
                }

                signal = shutdown.requested() => {
                    // Abandoning the wait costs redelivery of this batch and
                    // nothing else: it was never acknowledged, so the restart
                    // replays it.
                    return Ok(WaitOutcome::Shutdown(signal));
                }
            }
        };

        match outcome {
            Ok(waited) => {
                if waited >= SLOW_WAIT_WARNING {
                    eprintln!(
                        "WARNING: waited {waited:?} for Feldera; WAL is accumulating on the \
                         source while this blocks"
                    );
                }

                Ok(WaitOutcome::Durable(waited))
            }

            Err(SinkError::Unavailable(reason)) => Ok(WaitOutcome::Disconnected(reason)),

            Err(rejected) => Err(rejected.into()),
        }
    }

    /// Send everything in the outbox, wait for it to become durable, and only
    /// then tell PostgreSQL.
    ///
    /// The send and the acknowledgement are one operation on purpose. They were
    /// separate while a batch was always exactly one transaction, because the
    /// commit handler knew both that the batch was durable and which LSN that
    /// made safe. Once a batch can span several transactions the second half of
    /// that stops being true at the commit boundary: a COMMIT whose events are
    /// still sitting in the buffer must not be acknowledged, and the only place
    /// that knows the difference is here.
    ///
    /// Emptying the outbox is the *last* thing this does, and only on the path
    /// where durability was established. Everything else leaves the events
    /// where they are and ends the session, which is what makes a failed flush
    /// a redelivery: the slot was never advanced past them, so the replay
    /// brings them back.
    ///
    /// `Some(end)` means the session is over and the caller should return it.
    /// Returning it rather than acting on it keeps the loop in charge of its
    /// own exit while letting the final status update happen here, where the
    /// stream is already borrowed.
    pub(crate) async fn flush(
        &self,
        outbox: &mut Outbox,
        stream: &mut std::pin::Pin<&mut LogicalReplicationStream>,
        ticker: &mut tokio::time::Interval,
        position: &mut Position,
        shutdown: &mut Shutdown,
        batches: &mut u64,
    ) -> Result<Option<SessionEnd>, Box<dyn std::error::Error>> {
        if !outbox.events.is_empty() {
            // Counted here rather than at the commit boundary so a crash point
            // lands on something that actually moves data, whatever else the
            // workload happens to produce.
            *batches += 1;

            println!(
                "  Batch {}: sending {} events",
                *batches,
                outbox.events.len()
            );

            maybe_crash(&self.crash, CrashPoint::BeforeSend, *batches);

            for (route, payloads) in outbox.grouped() {
                let ingress = &self.routes[route].ingress_url;

                if self.routes.len() > 1 {
                    println!(
                        "    {} ({} event(s))",
                        self.routes[route].contract.table.name,
                        payloads.len()
                    );
                }

                let barrier = match self.sink.send(ingress, &payloads).await {
                    Ok(barrier) => barrier,

                    Err(SinkError::Unavailable(reason)) => {
                        return Ok(Some(SessionEnd::Disconnected(reason)));
                    }

                    Err(rejected) => return Err(rejected.into()),
                };

                maybe_crash(&self.crash, CrashPoint::AfterSend, *batches);

                let outcome = self
                    .wait_for_durability(barrier, stream, ticker, position, shutdown)
                    .await?;

                let waited = match outcome {
                    WaitOutcome::Durable(waited) => waited,

                    WaitOutcome::Disconnected(reason) => {
                        return Ok(Some(SessionEnd::Disconnected(reason)));
                    }

                    WaitOutcome::Shutdown(signal) => {
                        return Ok(Some(self.finish(stream, position, signal).await?));
                    }
                };

                println!("  Durable after {waited:?}");

                self.routes[route].remember_durable_events(&payloads);
            }

            maybe_crash(&self.crash, CrashPoint::AfterDurable, *batches);

            outbox.clear();
        }

        // Nothing is buffered now, so every transaction that has committed on
        // the stream so far is durable in Feldera, and that is the whole
        // content of the claim this update makes.
        //
        // The guard is not an optimisation. `committed_through` sits below the
        // position whenever a flush was triggered by the size bound in the
        // middle of a transaction, or by the timer with nothing new committed,
        // and writing it out then would drag the acknowledged position
        // backwards.
        if outbox.committed_through <= position.flush {
            return Ok(None);
        }

        position.flush = outbox.committed_through;

        if let Err(error) = send_standby_status(stream, position.received, position.flush, 0).await
        {
            return Ok(Some(SessionEnd::Disconnected(format!(
                "standby status update failed: {error}"
            ))));
        }

        // The periodic update exists to cover silence. This was not silence, so
        // restart the clock rather than send a second copy milliseconds later.
        ticker.reset();

        println!("Flush LSN advanced to {}", position.flush);

        maybe_crash(&self.crash, CrashPoint::AfterAck, *batches);

        Ok(None)
    }

    /// Open one replication connection and stream from it until it ends.
    ///
    /// The return type carries the retry decision. `Ok(SessionEnd)` is a
    /// failure a new connection could plausibly fix; `Err` is one that re-reads
    /// the same bytes and reaches the same conclusion, so retrying it only
    /// holds the slot open for longer.
    pub(crate) async fn run_session(
        &self,
        shutdown: &mut Shutdown,
        position: &mut Position,
        batches: &mut u64,
    ) -> Result<SessionEnd, Box<dyn std::error::Error>> {
        // --------------------------------------------------------
        // Connect
        // --------------------------------------------------------

        // The driver handle is dropped rather than awaited, which detaches the
        // task instead of stopping it: the connection has to outlive this
        // binding for the whole session, and it ends when `client` goes out of
        // scope at the end of the function.
        let (client, _driver) = match connect_pg(&self.replication_connection).await {
            Ok(pair) => pair,

            Err(error) => {
                return Ok(SessionEnd::Disconnected(format!(
                    "could not connect: {error}"
                )));
            }
        };

        // --------------------------------------------------------
        // Start logical replication
        // --------------------------------------------------------
        //
        // `0/0` asks the server to resume from the slot's
        // confirmed_flush_lsn, which is PostgreSQL's record of what this
        // connector last called safe. That is why reconnecting needs no
        // bookkeeping of its own: the position that matters was never held in
        // this process.

        let start = format!(
            "START_REPLICATION SLOT {} LOGICAL 0/0 (proto_version '1', publication_names '{}')",
            quote_ident(&self.slot),
            self.publication
        );

        let replication_stream = match client.copy_both_simple(&start).await {
            Ok(stream) => stream,

            Err(error) => {
                return Ok(SessionEnd::Disconnected(format!(
                    "START_REPLICATION failed: {error}"
                )));
            }
        };

        let mut stream = pin!(LogicalReplicationStream::new(replication_stream));

        println!("Streaming from slot {:?}", self.slot);

        // --------------------------------------------------------
        // Per-session state
        // --------------------------------------------------------
        //
        // Deliberately not carried across a reconnect. PostgreSQL re-sends
        // Relation messages on a new stream, so the cache costs nothing to
        // rebuild. A half-read transaction has to be dropped outright: the
        // replay starts at its BEGIN, and keeping the changes already buffered
        // would duplicate them *within* one transaction, where the primary key
        // is no defence because both copies are in the same batch.

        let mut relations: HashMap<u32, RelationMetadata> = HashMap::new();

        let mut current_transaction: Option<Transaction> = None;

        // Events converted and not yet durable. Dropped with the session for
        // the reason above: whatever is in here was never acknowledged, so the
        // replay produces it again, and carrying it across a reconnect would
        // splice the tail of the old stream onto the head of the new one.
        let mut outbox = Outbox::new();

        // Fires on an idle database, which is exactly when nothing else would
        // prompt a status update and when `wal_sender_timeout` is counting.
        let mut ticker = tokio::time::interval(self.status_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // --------------------------------------------------------
        // Replication loop
        // --------------------------------------------------------

        loop {
            // Only cancel-safe operations belong in these branches: two of the
            // three futures are dropped unfinished on every iteration.
            // `StreamExt::next` holds nothing but a borrow, `Interval::tick`
            // and `Signal::recv` are documented cancel safe, and each keeps its
            // state in the object rather than the future. The branch *bodies*
            // are a different matter — they run to completion once chosen,
            // which is what the nested select in the COMMIT arm is about.
            let message = tokio::select! {
                message = stream.next() => message,

                // The linger expiring. Disabled while nothing is buffered,
                // which is what keeps an idle connector from waking every
                // `batch_linger` to look at an empty buffer — and what makes it
                // safe for the deadline to be a stale `now` in that case, since
                // the future is created and then never polled.
                _ = tokio::time::sleep_until(outbox.deadline(self.batch_linger)),
                    if !outbox.events.is_empty() =>
                {
                    if let Some(end) = self
                        .flush(
                            &mut outbox,
                            &mut stream,
                            &mut ticker,
                            position,
                            shutdown,
                            batches,
                        )
                        .await?
                    {
                        return Ok(end);
                    }

                    continue;
                }

                _ = ticker.tick() => {
                    if let Err(error) =
                        send_standby_status(&mut stream, position.received, position.flush, 0).await
                    {
                        return Ok(SessionEnd::Disconnected(format!(
                            "standby status update failed: {error}"
                        )));
                    }

                    println!("Sent periodic standby status update");

                    continue;
                }

                signal = shutdown.requested() => {
                    return self.finish(&mut stream, position, signal).await;
                }
            };

            let message = match message {
                Some(Ok(message)) => message,

                Some(Err(error)) => {
                    return Ok(SessionEnd::Disconnected(format!("stream error: {error}")));
                }

                // The stream ending is not success. This used to be the loop's
                // exit condition, after which `main` returned `Ok`, so a
                // walsender killed at the far end left a zero exit status and
                // a supervisor with no reason to restart anything.
                None => {
                    return Ok(SessionEnd::Disconnected("stream ended".to_string()));
                }
            };

            match message {
                ReplicationMessage::XLogData(data) => {
                    // wal_start is this record's own position. wal_end is how
                    // far the server has written overall and is 0 on some
                    // messages, so assigning it directly would drag the
                    // reported position backwards; keeping the maximum makes it
                    // monotonic.
                    println!("Received WAL data at LSN {}", data.wal_start());
                    position.received = position.received.max(data.wal_start());

                    match data.data() {
                        // ====================================================
                        // RELATION
                        // ====================================================
                        LogicalReplicationMessage::Relation(relation) => {
                            println!("RELATION:");
                            println!("  ID: {}", relation.rel_id());
                            println!("  Schema: {:?}", relation.namespace());
                            println!("  Table: {:?}", relation.name());
                            println!("  Replica identity: {:?}", relation.replica_identity());

                            let schema = relation.namespace()?.to_string();
                            let table = relation.name()?.to_string();

                            let mut columns = Vec::new();

                            for column in relation.columns() {
                                println!("  Column:");
                                println!("    Name: {:?}", column.name());
                                println!("    Type ID: {}", column.type_id());
                                println!("    Flags: {}", column.flags());

                                columns.push(ColumnMetadata {
                                    name: column.name()?.to_string(),
                                    type_id: column.type_id(),
                                    // Flag bit 1 marks the column as part of
                                    // the replica identity.
                                    is_key: column.flags() & 1 == 1,
                                });
                            }

                            let metadata = RelationMetadata {
                                schema,
                                table,
                                columns,
                            };

                            // PostgreSQL re-sends this message whenever the
                            // table definition changes, so both of these see
                            // the identity as it was when each record was
                            // written rather than only the state at startup.
                            let route = route_index_for(&self.routes, &metadata)?;

                            refuse_unusable_identity(
                                &self.routes[route].contract,
                                &metadata.schema,
                                &metadata.table,
                                &metadata.columns,
                            )?;
                            report_identity_coverage(&self.routes[route].contract, &metadata);

                            relations.insert(relation.rel_id(), metadata);

                            println!("Stored relation metadata for ID {}", relation.rel_id());
                        }

                        // ====================================================
                        // BEGIN
                        // ====================================================
                        LogicalReplicationMessage::Begin(begin) => {
                            println!("BEGIN transaction");
                            println!("  XID: {}", begin.xid());
                            println!("  Final LSN: {}", begin.final_lsn());

                            current_transaction = Some(Transaction {
                                xid: begin.xid(),
                                changes: 0,
                                events: 0,
                            });
                        }

                        // ====================================================
                        // INSERT
                        // ====================================================
                        LogicalReplicationMessage::Insert(insert) => {
                            let relation_id = insert.rel_id();

                            println!("INSERT relation ID: {relation_id}");

                            let relation = relations.get(&relation_id).ok_or_else(|| {
                                format!(
                                    "Missing relation metadata for INSERT relation ID {}",
                                    relation_id
                                )
                            })?;

                            let row = decode_row(relation, insert.tuple().tuple_data())?;

                            let event = CdcEvent::Insert { row };
                            let route = route_index_for(&self.routes, relation)?;

                            buffer_change(
                                &mut outbox,
                                current_transaction.as_mut(),
                                "INSERT",
                                &event,
                                route,
                                &self.routes[route].contract,
                            )?;
                        }

                        // ====================================================
                        // UPDATE
                        // ====================================================
                        LogicalReplicationMessage::Update(update) => {
                            let relation_id = update.rel_id();

                            println!("UPDATE relation ID: {relation_id}");

                            let relation = relations.get(&relation_id).ok_or_else(|| {
                                format!(
                                    "Missing relation metadata for UPDATE relation ID {}",
                                    relation_id
                                )
                            })?;

                            let old = decode_previous_row(
                                relation,
                                update.old_tuple(),
                                update.key_tuple(),
                            )?;

                            let mut new_row =
                                decode_row(relation, update.new_tuple().tuple_data())?;

                            fill_toast_holes(&mut new_row, &old, relation)?;

                            let event = CdcEvent::Update { old, new_row };
                            let route = route_index_for(&self.routes, relation)?;

                            buffer_change(
                                &mut outbox,
                                current_transaction.as_mut(),
                                "UPDATE",
                                &event,
                                route,
                                &self.routes[route].contract,
                            )?;
                        }

                        // ====================================================
                        // DELETE
                        // ====================================================
                        LogicalReplicationMessage::Delete(delete) => {
                            let relation_id = delete.rel_id();

                            println!("DELETE relation ID: {relation_id}");

                            let relation = relations.get(&relation_id).ok_or_else(|| {
                                format!(
                                    "Missing relation metadata for DELETE relation ID {}",
                                    relation_id
                                )
                            })?;

                            let old = decode_previous_row(
                                relation,
                                delete.old_tuple(),
                                delete.key_tuple(),
                            )?;

                            let event = CdcEvent::Delete { old };
                            let route = route_index_for(&self.routes, relation)?;

                            buffer_change(
                                &mut outbox,
                                current_transaction.as_mut(),
                                "DELETE",
                                &event,
                                route,
                                &self.routes[route].contract,
                            )?;
                        }

                        // ====================================================
                        // COMMIT
                        // ====================================================
                        LogicalReplicationMessage::Commit(commit) => {
                            println!("COMMIT transaction");

                            println!("  Commit LSN: {}", commit.commit_lsn());
                            println!("  End LSN: {}", commit.end_lsn());

                            let Some(transaction) = current_transaction.take() else {
                                println!("WARNING: COMMIT received without active transaction");
                                continue;
                            };

                            println!(
                                "  XID {}: {} changes -> {} Feldera events",
                                transaction.xid, transaction.changes, transaction.events
                            );

                            // Recording the boundary is unconditional; acting on
                            // it is not. Everything up to this LSN is now
                            // buffered or already durable, so this is the
                            // furthest the position could legally move — and
                            // the flush is what decides whether it does, since
                            // only it knows whether the buffer actually drained.
                            outbox.committed_through = commit.end_lsn();

                            // A commit boundary is a place a batch *may* end,
                            // not one where it must. Sending here unconditionally
                            // is one round trip per source transaction, which
                            // measured at roughly 250 transactions per second no
                            // matter how small they were. Letting the next few
                            // join costs nothing but the linger, and costs no
                            // atomicity: whole transactions arriving in one
                            // request are applied in one step.
                            if outbox.due(self.batch_max_rows, self.batch_linger) {
                                if let Some(end) = self
                                    .flush(
                                        &mut outbox,
                                        &mut stream,
                                        &mut ticker,
                                        position,
                                        shutdown,
                                        batches,
                                    )
                                    .await?
                                {
                                    return Ok(end);
                                }
                            }
                        }

                        LogicalReplicationMessage::Truncate(truncate) => {
                            println!("TRUNCATE relation IDs: {:?}", truncate.rel_ids());

                            let targets = truncate_route_indices(
                                &self.routes,
                                &relations,
                                truncate.rel_ids(),
                            );

                            if targets.is_empty() {
                                continue;
                            }

                            if current_transaction.is_none() {
                                eprintln!(
                                    "WARNING: TRUNCATE arrived without a BEGIN; retracting anyway"
                                );
                            }

                            // Unsent changes in this transaction were discarded
                            // on the source by the TRUNCATE. Flushing them now
                            // would put rows into Feldera that the source just
                            // erased. They were never durable, so they are not
                            // in `session_keys` either.
                            if !outbox.events.is_empty() {
                                println!(
                                    "  Discarding {} unsent event(s); TRUNCATE discarded them \
                                     on the source",
                                    outbox.events.len()
                                );

                                outbox.clear();
                            }

                            for route in targets {
                                let deletes = match self.retract_events(&self.routes[route]).await {
                                    Ok(deletes) => deletes,

                                    Err(SinkError::Unavailable(reason)) => {
                                        return Ok(SessionEnd::Disconnected(reason));
                                    }

                                    Err(rejected) => return Err(rejected.into()),
                                };

                                println!(
                                    "  Retracting {} row(s) from {} (query + session keys)",
                                    deletes.len(),
                                    self.routes[route].contract.table.name
                                );

                                for chunk in deletes.chunks(self.batch_max_rows) {
                                    outbox.push(route, chunk.to_vec());

                                    if let Some(end) = self
                                        .flush(
                                            &mut outbox,
                                            &mut stream,
                                            &mut ticker,
                                            position,
                                            shutdown,
                                            batches,
                                        )
                                        .await?
                                    {
                                        return Ok(end);
                                    }
                                }
                            }
                        }

                        _ => {}
                    }

                    // The relief valve, and the one place this connector
                    // knowingly gives something up.
                    //
                    // A transaction bigger than the bound is sent in pieces,
                    // which means Feldera can be queried between them and see
                    // half of it. That is a real weakening of atomicity and it
                    // is chosen over the alternative, which is not "hold it
                    // whole" but "die": a source transaction has no size limit,
                    // so neither does a buffer that spans one, and a process
                    // killed for its memory replays the same transaction on
                    // restart and is killed again. An unbounded buffer does not
                    // preserve atomicity, it converts one oversized transaction
                    // into a permanently stuck connector.
                    //
                    // The position is deliberately not touched here. Feldera
                    // holding part of a transaction is recoverable precisely
                    // because PostgreSQL has not been told: the slot still
                    // points below the BEGIN, so a crash replays the whole
                    // transaction and the primary key absorbs the part that
                    // arrived twice.
                    if current_transaction.is_some() && outbox.events.len() >= self.batch_max_rows {
                        println!(
                            "  Outbox reached {} events mid-transaction; flushing early",
                            outbox.events.len()
                        );

                        if let Some(end) = self
                            .flush(
                                &mut outbox,
                                &mut stream,
                                &mut ticker,
                                position,
                                shutdown,
                                batches,
                            )
                            .await?
                        {
                            return Ok(end);
                        }
                    }
                }

                // ============================================================
                // KEEPALIVE
                // ============================================================
                ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                    println!("PostgreSQL keepalive: LSN {}", keepalive.wal_end());

                    // A keepalive says the walsender has nothing further to
                    // send up to wal_end, so every record below it is either
                    // already delivered or belongs to a table this publication
                    // does not carry. Adopting the position is what lets the
                    // slot advance on a database whose writes are all in other
                    // tables: without it confirmed_flush_lsn sticks at the last
                    // commit of the replicated table and PostgreSQL retains
                    // every segment written since, on behalf of a connector
                    // that is completely caught up.
                    //
                    // Safe because TCP delivers in order. Anything the server
                    // sent below wal_end was read before this message, so it
                    // has already been processed — or it is still open, which
                    // the guard covers. Mid-transaction the position would
                    // otherwise run ahead of changes that are received but not
                    // yet durable.
                    //
                    // The outbox has to be empty as well, and for exactly the
                    // same reason one level out. "Processed" used to mean
                    // "durable in Feldera", because every commit was flushed
                    // before the loop went back to reading. Now that a batch
                    // can wait for company, a keepalive can arrive between
                    // whole committed transactions whose events are still in
                    // the buffer, and adopting its position would acknowledge
                    // them before they were sent at all.
                    if current_transaction.is_none() && outbox.events.is_empty() {
                        position.received = position.received.max(keepalive.wal_end());
                        position.flush = position.flush.max(keepalive.wal_end());
                    }

                    if keepalive.reply() == 1 {
                        if let Err(error) =
                            send_standby_status(&mut stream, position.received, position.flush, 1)
                                .await
                        {
                            return Ok(SessionEnd::Disconnected(format!(
                                "standby status update failed: {error}"
                            )));
                        }

                        ticker.reset();

                        println!("Sent requested standby status update");
                    }
                }

                _ => {}
            }
        }
    }

    /// Stop at the point where restarting costs least.
    ///
    /// "Graceful" is not a feeling about tidiness. It has a measurable
    /// definition here: a graceful stop redelivers zero transactions, an abrupt
    /// one redelivers at least the last, and the difference is entirely whether
    /// the final acknowledgement reached the server before the process went
    /// away.
    pub(crate) async fn finish(
        &self,
        stream: &mut std::pin::Pin<&mut LogicalReplicationStream>,
        position: &Position,
        signal: &'static str,
    ) -> Result<SessionEnd, Box<dyn std::error::Error>> {
        println!("{signal} received; stopping");

        if let Err(error) = send_standby_status(stream, position.received, position.flush, 0).await
        {
            eprintln!(
                "WARNING: the final standby status update failed: {error}. Whatever PostgreSQL \
                 last recorded stands, so the transactions after it will be redelivered."
            );

            return Ok(SessionEnd::Shutdown(signal));
        }

        if position.flush == 0 {
            println!("Nothing was ever acknowledged, so there is nothing to confirm");

            return Ok(SessionEnd::Shutdown(signal));
        }

        // That `Ok` means less than it looks like. `CopyBothDuplex` implements
        // `Sink` over an in-process `mpsc::Sender` that the connection task
        // drains, so a successful send has moved the message into a channel and
        // nothing more: two hops remain, into the socket and across it. A
        // process that sends and exits immediately drops it. Measured, that is
        // not a rare race — the slot advanced zero times out of eight. Reading
        // the slot back over a second connection is the only confirmation the
        // protocol offers, because a standby status update has no reply.
        match confirm_slot_recorded(
            &self.base_connection,
            &self.slot,
            position.flush,
            SHUTDOWN_DEADLINE,
        )
        .await
        {
            Ok(true) => println!(
                "PostgreSQL recorded flush LSN {}; restarting from here redelivers nothing",
                PgLsn::from(position.flush)
            ),

            Ok(false) => eprintln!(
                "WARNING: PostgreSQL had not recorded flush LSN {} within {:?}. Exiting anyway, \
                 because a longer wait is a hang and the next signal is SIGKILL; the transactions \
                 past the slot's position will be redelivered and the primary key will absorb \
                 them.",
                PgLsn::from(position.flush),
                SHUTDOWN_DEADLINE
            ),

            Err(error) => eprintln!(
                "WARNING: could not confirm the final acknowledgement: {error}. The transactions \
                 past the slot's position may be redelivered."
            ),
        }

        Ok(SessionEnd::Shutdown(signal))
    }
}
