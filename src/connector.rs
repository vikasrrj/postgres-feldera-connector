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
    pub(crate) async fn prepare(
        &self,
        shutdown: &mut Shutdown,
    ) -> Result<Progress<()>, Box<dyn std::error::Error>> {
        // `never` reads no state, so an unreadable file left by a differently
        // configured run cannot fail a mode that would not have consulted it.
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
    /// created it stays open.
    pub(crate) async fn create_slot(
        &self,
        export_snapshot: bool,
    ) -> Result<(Client, tokio::task::JoinHandle<()>, CreatedSlot), Box<dyn std::error::Error>>
    {
        let (client, driver) = connect_pg(&self.replication_connection).await?;

        // export is the default and pins xmin for the life of the connection,
        // so it is only asked for when it will actually be read.
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

            SnapshotMode::Never => {
                Err("SNAPSHOT_MODE=never asked for a snapshot, which is a bug".into())
            }
        }
    }

    /// Read the table as it stood at the slot's consistent point.
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

        // Written before any rows move so a crash leaves "pending" and the
        // next run reads the table again.
        write_snapshot_state(
            &self.snapshot_state_path,
            &SnapshotState {
                slot: self.slot.clone(),
                consistent_point: created.consistent_point,
                complete: false,
            },
        )?;

        let (reader, reader_driver) = connect_pg(&self.base_connection).await?;

        // SET TRANSACTION SNAPSHOT needs a transaction whose snapshot is taken
        // once and reused, which is what REPEATABLE READ provides.
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

        // Commit before the connection closes so the snapshot is not held to
        // block vacuum.
        reader.batch_execute("commit").await?;

        drop(reader);
        let _ = reader_driver.await;

        // Only now may the replication connection go: the COPY is the thing
        // that needed its exported snapshot alive.
        drop(replication_client);
        let _ = replication_driver.await;

        let rows = match outcome {
            Progress::Done(rows) => rows,

            // Left "pending" so the next run reads the table again.
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

    // Snapshot-before-slot loses the window between the read and the slot's
    // consistent point; kept only so the loss can be demonstrated.
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

        // No "pending" state is recorded on the way in, because until the slot
        // exists there is no consistent point to record.
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
    /// Retract all, then COPY all, so the two tables never describe different
    /// moments for the length of the second COPY.
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

        // Explicit column list keeps COPY order equal to the catalog query order.
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

        // Chunks do not align to row boundaries, so a partial row at the end
        // of a chunk is kept until the rest turns up.
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

        // COPY terminates every row with a newline, so anything left is a row
        // the server never finished sending.
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

    /// Deliver one snapshot batch, retrying while the failure is retryable.
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
            // Abandoning a send needs no cleanup: the state file still says
            // "pending", so a restart reads the table again.
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

                // Identical bytes to an endpoint that already refused them.
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
                    // The old flush position; a keepalive that lies is worse
                    // than none.
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
                    // Never acknowledged, so a restart replays this batch.
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
            // Counted here so a crash point lands on something that actually
            // moves data.
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

        // Nothing is buffered now, so the position may move to the furthest
        // committed LSN. Never move it backwards when a flush fires with
        // nothing new committed.
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

        // Not silence, so restart the clock rather than send a second copy.
        ticker.reset();

        println!("Flush LSN advanced to {}", position.flush);

        maybe_crash(&self.crash, CrashPoint::AfterAck, *batches);

        Ok(None)
    }

    /// Open one replication connection and stream from it until it ends.
    pub(crate) async fn run_session(
        &self,
        shutdown: &mut Shutdown,
        position: &mut Position,
        batches: &mut u64,
    ) -> Result<SessionEnd, Box<dyn std::error::Error>> {
        // --------------------------------------------------------
        // Connect
        // --------------------------------------------------------

        // The driver handle is dropped rather than awaited, so the connection
        // task outlives this binding for the whole session.
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
        // 0/0 asks the server to resume from the slot's confirmed_flush_lsn.

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
        // Rebuilt per session: PostgreSQL re-sends RELATION messages on a new
        // stream, and the replay restarts at each BEGIN.

        let mut relations: HashMap<u32, RelationMetadata> = HashMap::new();

        let mut current_transaction: Option<Transaction> = None;

        // Events converted and not yet durable; dropped with the session,
        // since what is in here was never acknowledged.
        let mut outbox = Outbox::new();

        // Fires on an idle database, when nothing else would prompt a status
        // update and wal_sender_timeout is counting.
        let mut ticker = tokio::time::interval(self.status_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // --------------------------------------------------------
        // Replication loop
        // --------------------------------------------------------

        loop {
            // Only cancel-safe futures belong here; several are dropped
            // unfinished on every iteration.
            let message = tokio::select! {
                message = stream.next() => message,

                // Disabled while nothing is buffered, which keeps an idle
                // connector from waking every batch_linger.
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

                // The stream ending is not success; a walsender killed at the
                // far end must not leave a zero exit status.
                None => {
                    return Ok(SessionEnd::Disconnected("stream ended".to_string()));
                }
            };

            match message {
                ReplicationMessage::XLogData(data) => {
                    // Keep the max of wal_end; it is 0 on some messages, so
                    // assigning it directly would drag the position backwards.
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
                                    // Flag bit 1 marks the column as part of the
                                    // replica identity.
                                    is_key: column.flags() & 1 == 1,
                                });
                            }

                            let metadata = RelationMetadata {
                                schema,
                                table,
                                columns,
                            };

                            // PostgreSQL resends this when the table definition
                            // changes.
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

                            // Recording the boundary is unconditional; the
                            // flush decides whether to act on it.
                            outbox.committed_through = commit.end_lsn();

                            // A commit is a place a batch may end, not must;
                            // the size bound and the linger decide.
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

                            // TRUNCATE discarded the unsent change on the
                            // source, so play nothing of it forward.
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

                    // A transaction bigger than the bound is sent in pieces,
                    // weakening atomicity but not dying: an unbounded buffer
                    // converts one oversized transaction into a permanently
                    // stuck connector. The position is deliberately left put,
                    // so a crash replays the whole transaction and the primary
                    // key absorbs the part that arrived twice.
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

                    // A keepalive says nothing further is coming up to wal_end,
                    // so the slot can advance even on a database whose writes
                    // are all in other tables. Safe because TCP delivers in
                    // order; only claimed when no transaction is open and
                    // nothing is buffered, so the position never runs ahead of
                    // changes that are received but not yet durable.
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

        // A successful send only moved the message into a channel; two hops
        // remain, into the socket and across it. Reading the slot back over a
        // second connection is the only confirmation the protocol offers,
        // because a standby status update has no reply.
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