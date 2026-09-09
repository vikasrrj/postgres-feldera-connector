use std::collections::HashMap;
use std::pin::pin;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use postgres_replication::LogicalReplicationStream;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage, TupleData};
use tokio_postgres::NoTls;
use tokio_postgres::types::PgLsn;

#[derive(Debug)]
struct RelationMetadata {
    schema: String,
    table: String,
    columns: Vec<ColumnMetadata>,
}

#[derive(Debug)]
struct ColumnMetadata {
    name: String,
    type_id: i32,
    /// Set when the column belongs to the table's replica identity. Under
    /// REPLICA IDENTITY FULL that is every column; otherwise it is the identity
    /// index, which is the only case where this distinction is useful.
    is_key: bool,
}

#[derive(Debug)]
enum Value {
    Int32(i32),
    Text(String),
    Null,
}

#[derive(Debug)]
struct Row {
    values: Vec<(String, Value)>,
}

/// What PostgreSQL logged about a row as it was before an UPDATE or DELETE.
///
/// Which variant arrives is decided entirely by the table's REPLICA IDENTITY,
/// and PostgreSQL sends at most one of the two tuples. Reading which one turned
/// up is therefore a direct answer to "did the replica identity change?" — no
/// comparison of old and new values is needed.
#[derive(Debug)]
enum PreviousRow {
    /// Every column of the previous row, sent under REPLICA IDENTITY FULL.
    Full(Row),
    /// Only the replica identity columns, sent when the identity changed and
    /// the table is not REPLICA IDENTITY FULL.
    Key(Row),
    /// Nothing was logged, which means the replica identity did not change.
    Absent,
}

#[derive(Debug)]
enum CdcEvent {
    Insert {
        schema: String,
        table: String,
        row: Row,
    },

    Update {
        schema: String,
        table: String,
        old: PreviousRow,
        new_row: Row,
    },

    Delete {
        schema: String,
        table: String,
        old: PreviousRow,
    },
}

#[derive(Debug)]
struct Transaction {
    xid: u32,
    changes: Vec<CdcEvent>,
}

// ============================================================
// ROW DECODING
// ============================================================

fn decode_value(
    column: &ColumnMetadata,
    index: usize,
    value: &TupleData,
) -> Result<Value, Box<dyn std::error::Error>> {
    let decoded_value = match column.type_id {
        23 => match value {
            TupleData::Text(bytes) => {
                let number = std::str::from_utf8(bytes)?.parse::<i32>()?;
                Value::Int32(number)
            }

            TupleData::Null => Value::Null,

            _ => {
                return Err(format!(
                    "Column {} ({}): unsupported value format",
                    index, column.name
                )
                .into());
            }
        },

        25 | 1043 => match value {
            TupleData::Text(bytes) => {
                let text = std::str::from_utf8(bytes)?.to_string();
                Value::Text(text)
            }

            TupleData::Null => Value::Null,

            _ => {
                return Err(format!(
                    "Column {} ({}): unsupported value format",
                    index, column.name
                )
                .into());
            }
        },

        _ => {
            return Err(format!(
                "Column {} ({}): unsupported PostgreSQL type {}",
                index, column.name, column.type_id
            )
            .into());
        }
    };

    Ok(decoded_value)
}

/// Every tuple in the stream carries one slot per column of the relation, so a
/// width mismatch means our cached metadata is stale and nothing can be decoded
/// safely.
fn check_tuple_width(
    relation: &RelationMetadata,
    tuple_values: &[TupleData],
) -> Result<(), Box<dyn std::error::Error>> {
    if tuple_values.len() != relation.columns.len() {
        return Err(format!(
            "Tuple contains {} values but relation {}.{} has {} columns",
            tuple_values.len(),
            relation.schema,
            relation.table,
            relation.columns.len()
        )
        .into());
    }

    Ok(())
}

fn decode_row(
    relation: &RelationMetadata,
    tuple_values: &[TupleData],
) -> Result<Row, Box<dyn std::error::Error>> {
    check_tuple_width(relation, tuple_values)?;

    let mut values = Vec::new();

    for (index, value) in tuple_values.iter().enumerate() {
        let column = &relation.columns[index];

        values.push((column.name.clone(), decode_value(column, index, value)?));
    }

    Ok(Row { values })
}

/// Decode a key tuple, keeping only the replica identity columns.
///
/// A key tuple is not a short tuple: PostgreSQL sends a slot for every column
/// and fills the non-identity ones with NULL. Forwarding it whole would claim
/// the row's other columns really are NULL, so the non-identity slots have to be
/// dropped rather than decoded.
fn decode_key_row(
    relation: &RelationMetadata,
    tuple_values: &[TupleData],
) -> Result<Row, Box<dyn std::error::Error>> {
    check_tuple_width(relation, tuple_values)?;

    let mut values = Vec::new();

    for (index, value) in tuple_values.iter().enumerate() {
        let column = &relation.columns[index];

        if !column.is_key {
            continue;
        }

        values.push((column.name.clone(), decode_value(column, index, value)?));
    }

    if values.is_empty() {
        return Err(format!(
            "Relation {}.{} sent a key tuple but declares no replica identity columns",
            relation.schema, relation.table
        )
        .into());
    }

    Ok(Row { values })
}

/// Read which pre-image PostgreSQL actually sent for an UPDATE or DELETE.
///
/// The server emits at most one of the two tuples and chooses between them from
/// the table's REPLICA IDENTITY, so the variant that turns up is itself the
/// answer to whether the identity changed.
fn decode_previous_row(
    relation: &RelationMetadata,
    old_tuple: Option<&postgres_replication::protocol::Tuple>,
    key_tuple: Option<&postgres_replication::protocol::Tuple>,
) -> Result<PreviousRow, Box<dyn std::error::Error>> {
    match (old_tuple, key_tuple) {
        (Some(tuple), _) => Ok(PreviousRow::Full(decode_row(relation, tuple.tuple_data())?)),
        (None, Some(tuple)) => Ok(PreviousRow::Key(decode_key_row(
            relation,
            tuple.tuple_data(),
        )?)),
        (None, None) => Ok(PreviousRow::Absent),
    }
}

// ============================================================
// JSON CONVERSION
// ============================================================

fn row_to_json(row: &Row) -> serde_json::Value {
    let mut object = serde_json::Map::new();

    for (name, value) in &row.values {
        let json_value = match value {
            Value::Int32(value) => serde_json::json!(value),
            Value::Text(value) => serde_json::json!(value),
            Value::Null => serde_json::Value::Null,
        };

        object.insert(name.clone(), json_value);
    }

    serde_json::Value::Object(object)
}

// ============================================================
// FELDERA
// ============================================================

/// Feldera's insert/delete format requires every data change event to be a JSON
/// object with exactly one key. A single WAL change can therefore need more than
/// one event: replacing a row is expressed as a delete followed by an insert.
fn feldera_change_events(
    event: &CdcEvent,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let events = match event {
        CdcEvent::Insert { row, .. } => {
            vec![serde_json::json!({ "insert": row_to_json(row) })]
        }

        CdcEvent::Delete { old, .. } => {
            let row = match old {
                PreviousRow::Full(row) | PreviousRow::Key(row) => row,

                PreviousRow::Absent => {
                    return Err(
                        "DELETE carried no previous row: the table needs a replica identity".into(),
                    );
                }
            };

            vec![serde_json::json!({ "delete": row_to_json(row) })]
        }

        // Retracting the previous row and inserting the new one is correct
        // whichever tuple arrived. Feldera counts occurrences of a row, so when
        // the identity is unchanged the delete and insert cancel down to exactly
        // an update, and when it changed the old identity is properly retired.
        CdcEvent::Update { old, new_row, .. } => match old {
            PreviousRow::Full(old_row) | PreviousRow::Key(old_row) => vec![
                serde_json::json!({ "delete": row_to_json(old_row) }),
                serde_json::json!({ "insert": row_to_json(new_row) }),
            ],

            PreviousRow::Absent => {
                vec![serde_json::json!({ "insert": row_to_json(new_row) })]
            }
        },
    };

    Ok(events)
}

/// Newline-delimited JSON, which is how Feldera expects several change events to
/// arrive in one request body when the endpoint is called with `array=false`.
fn encode_change_events(events: &[serde_json::Value]) -> String {
    events
        .iter()
        .map(|event| event.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

// ============================================================
// DURABILITY
// ============================================================

/// When the connector is allowed to tell PostgreSQL that a batch is safe.
///
/// The distinction exists because a 200 from `/ingress` only means Feldera
/// parsed the batch into an input buffer. Acknowledging on that basis lets
/// PostgreSQL delete the WAL while the data still lives nowhere but that
/// buffer.
#[derive(Debug, Clone, Copy, PartialEq)]
enum AckMode {
    /// Treat acceptance as safety. Loses data if the pipeline dies with the
    /// buffer unprocessed. Kept so the loss can be demonstrated rather than
    /// argued about.
    OnAccepted,
    /// Wait for Feldera to report the batch fully processed and written to
    /// every attached sink.
    OnCompleted,
}

/// Handed back by [`FelderaSink::send`]. Holding one means the batch was
/// accepted, which is explicitly *not* the same as being safe to acknowledge —
/// [`FelderaSink::wait`] is what establishes that.
#[derive(Debug)]
enum Barrier {
    /// Nothing left to wait for, because this mode equates acceptance with
    /// safety.
    AlreadyResolved,
    /// Feldera issued a completion token, and safety means polling it.
    AwaitingCompletion { token: String },
}

/// Polling interval for `/completion_status`, doubling from the first to the
/// second so a fast pipeline is not delayed and a slow one is not hammered.
const COMPLETION_POLL_MIN: Duration = Duration::from_millis(5);
const COMPLETION_POLL_MAX: Duration = Duration::from_millis(250);

/// A wait beyond this is worth reporting: unacknowledged data is WAL the source
/// server cannot free, so a slow sink shows up as disk pressure on PostgreSQL.
const SLOW_WAIT_WARNING: Duration = Duration::from_secs(5);

struct FelderaSink {
    client: reqwest::Client,
    ingress_url: String,
    completion_status_url: String,
    ack_mode: AckMode,
    completion_timeout: Duration,
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

impl FelderaSink {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let base = env_or("FELDERA_URL", "http://localhost:8080");
        let pipeline = env_or("FELDERA_PIPELINE", "JVM");
        let table = env_or("FELDERA_TABLE", "users");

        let ack_mode = match env_or("FELDERA_ACK_MODE", "completed").as_str() {
            "completed" => AckMode::OnCompleted,
            "accepted" => AckMode::OnAccepted,
            other => {
                return Err(format!(
                    "FELDERA_ACK_MODE must be \"completed\" or \"accepted\", got {other:?}"
                )
                .into());
            }
        };

        let completion_timeout =
            Duration::from_secs(env_or("FELDERA_COMPLETION_TIMEOUT_SECS", "60").parse::<u64>()?);

        Ok(Self {
            client: reqwest::Client::new(),
            ingress_url: format!(
                "{base}/v0/pipelines/{pipeline}/ingress/{table}\
                 ?format=json&update_format=insert_delete&array=false"
            ),
            completion_status_url: format!("{base}/v0/pipelines/{pipeline}/completion_status"),
            ack_mode,
            completion_timeout,
        })
    }

    /// Submit one batch. Returning `Ok` means Feldera accepted it, nothing more.
    async fn send(
        &self,
        events: &[serde_json::Value],
    ) -> Result<Barrier, Box<dyn std::error::Error>> {
        let response = self
            .client
            .post(&self.ingress_url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(encode_change_events(events))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await?;

            return Err(format!("Feldera rejected the batch: {status} - {body}").into());
        }

        match self.ack_mode {
            AckMode::OnAccepted => Ok(Barrier::AlreadyResolved),

            AckMode::OnCompleted => {
                let payload: serde_json::Value = response.json().await?;

                // A missing token must fail rather than quietly fall back to
                // treating acceptance as safety, which is the exact bug this
                // whole mechanism exists to prevent.
                let token = payload
                    .get("token")
                    .and_then(|token| token.as_str())
                    .ok_or_else(|| {
                        format!(
                            "Feldera accepted the batch but returned no completion token: {payload}"
                        )
                    })?;

                Ok(Barrier::AwaitingCompletion {
                    token: token.to_string(),
                })
            }
        }
    }

    /// Block until the batch behind `barrier` is safe to acknowledge, and report
    /// how long that took.
    async fn wait(&self, barrier: Barrier) -> Result<Duration, Box<dyn std::error::Error>> {
        let started = Instant::now();

        let token = match barrier {
            Barrier::AlreadyResolved => return Ok(Duration::ZERO),
            Barrier::AwaitingCompletion { token } => token,
        };

        let mut delay = COMPLETION_POLL_MIN;

        loop {
            let response = self
                .client
                .get(&self.completion_status_url)
                // Tokens are base64 and contain padding, so let the query
                // builder percent-encode rather than pasting into the URL.
                .query(&[("token", token.as_str())])
                .send()
                .await?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await?;

                return Err(format!("Feldera completion check failed: {status} - {body}").into());
            }

            let payload: serde_json::Value = response.json().await?;

            match payload.get("status").and_then(|status| status.as_str()) {
                Some("complete") => return Ok(started.elapsed()),

                Some("inprogress") => {}

                Some(other) => {
                    return Err(format!("Feldera reported unknown status {other:?}").into());
                }

                None => {
                    return Err(
                        format!("Feldera completion check returned no status: {payload}").into(),
                    );
                }
            }

            if started.elapsed() >= self.completion_timeout {
                return Err(format!(
                    "Feldera did not report completion within {:?}. \
                     PostgreSQL is not being acknowledged, so this transaction \
                     will be redelivered on restart.",
                    self.completion_timeout
                )
                .into());
            }

            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(COMPLETION_POLL_MAX);
        }
    }
}

/// Seconds between the Unix epoch (1970-01-01)
/// and the Postgres epoch (2000-01-01).
const POSTGRES_EPOCH_OFFSET_SECONDS: u64 = 946_684_800;

/// Microseconds since 2000-01-01 00:00:00 UTC.
fn postgres_timestamp_now() -> i64 {
    let postgres_epoch = UNIX_EPOCH + Duration::from_secs(POSTGRES_EPOCH_OFFSET_SECONDS);

    SystemTime::now()
        .duration_since(postgres_epoch)
        .expect("system clock is before 2000-01-01")
        .as_micros() as i64
}

async fn send_standby_status(
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
// MAIN
// ============================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --------------------------------------------------------
    // Sink
    // --------------------------------------------------------

    let sink = FelderaSink::from_env()?;

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
    // PostgreSQL connection
    // --------------------------------------------------------

    let (client, connection) = tokio_postgres::connect(
        "host=localhost user=postgres password=postgres replication=database",
        NoTls,
    )
    .await?;

    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("PostgreSQL connection error: {error}");
        }
    });

    println!("PostgreSQL connection established");

    // --------------------------------------------------------
    // Start logical replication
    // --------------------------------------------------------

    let replication_stream = client
        .copy_both_simple(
            "START_REPLICATION SLOT rust_slot LOGICAL 0/0 \
             (proto_version '1', publication_names 'my_pub')",
        )
        .await?;

    let mut stream = pin!(LogicalReplicationStream::new(replication_stream));

    println!("Logical replication stream started");

    // --------------------------------------------------------
    // State
    // --------------------------------------------------------

    let mut relations: HashMap<u32, RelationMetadata> = HashMap::new();

    let mut current_transaction: Option<Transaction> = None;

    let mut received_lsn: u64 = 0;
    let mut flush_lsn: u64 = 0;
    let mut last_status_sent = Instant::now();

    // --------------------------------------------------------
    // Replication loop
    // --------------------------------------------------------

    while let Some(message) = stream.next().await {
        match message? {
            ReplicationMessage::XLogData(data) => {
                // wal_start is this record's own position. wal_end is how far
                // the server has written overall and is 0 on some messages, so
                // assigning it directly would drag the reported position
                // backwards; keeping the maximum makes it monotonic.
                println!("Received WAL data at LSN {}", data.wal_start());
                received_lsn = received_lsn.max(data.wal_start());

                match data.data() {
                    // ====================================================
                    // RELATION
                    // ====================================================
                    LogicalReplicationMessage::Relation(relation) => {
                        println!("RELATION:");
                        println!("  ID: {}", relation.rel_id());
                        println!("  Schema: {:?}", relation.namespace());
                        println!("  Table: {:?}", relation.name());

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

                        relations.insert(
                            relation.rel_id(),
                            RelationMetadata {
                                schema,
                                table,
                                columns,
                            },
                        );

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
                            changes: Vec::new(),
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

                        let event = CdcEvent::Insert {
                            schema: relation.schema.clone(),
                            table: relation.table.clone(),
                            row,
                        };

                        current_transaction
                            .as_mut()
                            .ok_or("INSERT received outside a transaction")?
                            .changes
                            .push(event);
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

                        let old =
                            decode_previous_row(relation, update.old_tuple(), update.key_tuple())?;

                        let new_row = decode_row(relation, update.new_tuple().tuple_data())?;

                        let event = CdcEvent::Update {
                            schema: relation.schema.clone(),
                            table: relation.table.clone(),
                            old,
                            new_row,
                        };

                        current_transaction
                            .as_mut()
                            .ok_or("UPDATE received outside a transaction")?
                            .changes
                            .push(event);
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

                        let old =
                            decode_previous_row(relation, delete.old_tuple(), delete.key_tuple())?;

                        let event = CdcEvent::Delete {
                            schema: relation.schema.clone(),
                            table: relation.table.clone(),
                            old,
                        };

                        current_transaction
                            .as_mut()
                            .ok_or("DELETE received outside a transaction")?
                            .changes
                            .push(event);
                    }

                    // ====================================================
                    // COMMIT
                    // ====================================================
                    LogicalReplicationMessage::Commit(commit) => {
                        println!("COMMIT transaction");

                        println!("  Commit LSN: {}", commit.commit_lsn());
                        println!("  End LSN: {}", commit.end_lsn());

                        if let Some(transaction) = current_transaction.take() {
                            // One request per transaction rather than per change.
                            // The transaction is what PostgreSQL commits
                            // atomically and what gets replayed on restart, so it
                            // is the meaningful unit to make durable — and it
                            // yields one completion token instead of one per row.
                            let mut events = Vec::new();

                            for change in &transaction.changes {
                                events.extend(feldera_change_events(change)?);
                            }

                            println!(
                                "  XID {}: {} changes -> {} Feldera events",
                                transaction.xid,
                                transaction.changes.len(),
                                events.len()
                            );

                            // An empty transaction still has to advance the
                            // position. One that touched only unpublished tables
                            // produces no events, and withholding the
                            // acknowledgement would stall the slot for good.
                            if !events.is_empty() {
                                let barrier = sink.send(&events).await?;
                                let waited = sink.wait(barrier).await?;

                                if waited >= SLOW_WAIT_WARNING {
                                    eprintln!(
                                        "WARNING: waited {waited:?} for Feldera; WAL is \
                                         accumulating on the source while this blocks"
                                    );
                                }

                                println!("  Durable after {waited:?}");
                            }

                            // Only now is the claim true.
                            flush_lsn = commit.end_lsn();

                            send_standby_status(&mut stream, received_lsn, flush_lsn, 0).await?;

                            last_status_sent = Instant::now();

                            println!("Flush LSN advanced to {}", flush_lsn);
                        } else {
                            println!("WARNING: COMMIT received without active transaction");
                        }
                    }

                    _ => {}
                }
            }

            // ============================================================
            // KEEPALIVE
            // ============================================================
            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                println!("PostgreSQL keepalive: LSN {}", keepalive.wal_end());

                if keepalive.reply() == 1 {
                    send_standby_status(&mut stream, received_lsn, flush_lsn, 1).await?;

                    last_status_sent = Instant::now();

                    println!("Sent requested standby status update");
                }
            }

            _ => {}
        }

        // ============================================================
        // PERIODIC STANDBY STATUS
        // ============================================================
        //
        // Checked once per message rather than inside the XLogData arm, because
        // an idle database sends only keepalives and idle is exactly when this
        // timer has a job to do.
        if last_status_sent.elapsed() >= Duration::from_secs(10) {
            send_standby_status(&mut stream, received_lsn, flush_lsn, 0).await?;

            last_status_sent = Instant::now();

            println!("Sent periodic standby status update");
        }
    }

    Ok(())
}
