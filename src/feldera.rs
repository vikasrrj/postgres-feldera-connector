//! Feldera HTTP sink: ingress, completion tokens, ad-hoc query.

use std::time::{Duration, Instant};

use crate::config::env_or;
use crate::contract::{SqlIdentifier, TargetContract, check_delete_carries_key};
use crate::decode::{PreviousRow, Row, quote_ident, row_to_json};

/// One change to the replicated table, decoded and not yet translated.
///
/// The three variants are what pgoutput can say about a row, and the shape of
/// each is what the translation needs: an insert carries only the new row, a
/// delete only the previous one, and an update both, because retiring the old
/// identity and adding the new one are separate facts.
///
/// The table is not on the event. Routing is decided once, at the RELATION,
/// and the outbox entry carries the route index. Repeating the name on every
/// change would clone a constant in the hot path for a fact the cache already
/// holds.
#[derive(Debug)]
pub(crate) enum CdcEvent {
    Insert { row: Row },

    Update { old: PreviousRow, new_row: Row },

    Delete { old: PreviousRow },
}

// ============================================================
// FELDERA
// ============================================================

/// Feldera's insert/delete format requires every data change event to be a JSON
/// object with exactly one key. A single WAL change can therefore need more than
/// one event: replacing a row is expressed as a delete followed by an insert.
pub(crate) fn feldera_change_events(
    event: &CdcEvent,
    contract: &TargetContract,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let events = match event {
        CdcEvent::Insert { row } => {
            vec![serde_json::json!({ "insert": row_to_json(row)? })]
        }

        CdcEvent::Delete { old } => {
            let row = match old {
                PreviousRow::Full(row) | PreviousRow::Key(row) => row,

                PreviousRow::Absent => {
                    return Err(
                        "DELETE carried no previous row: the table needs a replica identity".into(),
                    );
                }
            };

            check_delete_carries_key(contract, row)?;

            vec![serde_json::json!({ "delete": row_to_json(row)? })]
        }

        // Retracting the previous row and inserting the new one is correct
        // whichever tuple arrived. Feldera counts occurrences of a row, so when
        // the identity is unchanged the delete and insert cancel down to exactly
        // an update, and when it changed the old identity is properly retired.
        CdcEvent::Update { old, new_row } => match old {
            PreviousRow::Full(old_row) | PreviousRow::Key(old_row) => {
                check_delete_carries_key(contract, old_row)?;

                vec![
                    serde_json::json!({ "delete": row_to_json(old_row)? }),
                    serde_json::json!({ "insert": row_to_json(new_row)? }),
                ]
            }

            PreviousRow::Absent => {
                vec![serde_json::json!({ "insert": row_to_json(new_row)? })]
            }
        },
    };

    Ok(events)
}

/// Converted events waiting to be sent, and what acknowledging them would mean.
///
/// The two fields answer different questions and both are needed. `events` is
/// what goes in the request body and is what the size bound limits.
/// `committed_through` is what may be acknowledged once that request is durable
/// — which is not the same thing, because the buffer can hold a transaction
/// that has not committed yet, and the position must never move past a BEGIN
/// whose COMMIT has not been seen.
pub(crate) fn encode_change_events(events: &[serde_json::Value]) -> String {
    use std::fmt::Write;

    let mut encoded = String::new();

    for (index, event) in events.iter().enumerate() {
        if index > 0 {
            encoded.push('\n');
        }

        // `Display` for `Value` is the compact form, and writing through
        // `fmt::Write` into an existing `String` cannot fail — the trait is
        // fallible only because other implementors write to fallible sinks.
        let _ = write!(encoded, "{event}");
    }

    encoded
}
pub(crate) fn unwrap_query_row(value: serde_json::Value) -> Option<serde_json::Value> {
    match value {
        serde_json::Value::Null => None,

        serde_json::Value::Object(mut fields) => {
            if let Some(inner) = fields.remove("insert").or_else(|| fields.remove("delete")) {
                return Some(inner);
            }

            Some(serde_json::Value::Object(fields))
        }

        other => Some(other),
    }
}

/// Parse Feldera's `/query` body as NDJSON or as a JSON array.
///
/// `format=json` is documented as newline-delimited records. Some deployments
/// still wrap the same records in `[...]`, and treating that as one broken
/// line used to make the connector decide Feldera was empty.
pub(crate) fn parse_query_rows(body: &str) -> Result<Vec<serde_json::Value>, SinkError> {
    let trimmed = body.trim();

    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    if trimmed.starts_with('[') {
        let values: Vec<serde_json::Value> = serde_json::from_str(trimmed).map_err(|error| {
            SinkError::Rejected(format!(
                "Feldera query returned a JSON array that did not parse: {error}"
            ))
        })?;

        return Ok(values.into_iter().filter_map(unwrap_query_row).collect());
    }

    let mut rows = Vec::new();

    for (index, line) in trimmed.lines().enumerate() {
        let line = line.trim();

        if line.is_empty() {
            continue;
        }

        let value: serde_json::Value = serde_json::from_str(line).map_err(|error| {
            SinkError::Rejected(format!(
                "Feldera query row {} was not JSON: {error}: {line}",
                index + 1
            ))
        })?;

        if let Some(row) = unwrap_query_row(value) {
            rows.push(row);
        }
    }

    Ok(rows)
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
pub(crate) enum AckMode {
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
pub(crate) enum Barrier {
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
pub(crate) const SLOW_WAIT_WARNING: Duration = Duration::from_secs(5);

/// The most Feldera events one request will carry.
///
/// This is a memory bound, not a tuning knob, and it is the reason the
/// connector survives a transaction larger than RAM. It is expressed in events
/// rather than bytes because the row width is fixed by the table and a count is
/// something an operator can reason about; at the ~2 kB per event measured for
/// a two-column table, ten thousand events is roughly 20 MB in flight.
pub(crate) const BATCH_MAX_ROWS_DEFAULT: &str = "10000";

/// How long a batch waits for company before it is sent.
///
/// One request per source transaction costs a full round trip per transaction,
/// which measured out at a ceiling near 250 transactions per second regardless
/// of how small they were. Waiting a little lets consecutive transactions share
/// one request and one completion token, and unlike splitting a transaction it
/// costs no atomicity at all: a batch spanning three whole transactions is
/// applied by Feldera in one step, so the boundaries that disappear are ones no
/// reader could have observed between anyway.
///
/// Zero disables it, which is exactly one request per transaction again.
pub(crate) const BATCH_LINGER_MS_DEFAULT: &str = "50";

/// Whether a Feldera failure is worth retrying.
///
/// The classification is the whole point of the type. Reconnecting re-reads the
/// replication stream from the slot's position and re-sends the same
/// transaction, so a retry helps exactly when the failure was about *reaching*
/// Feldera, and never when it was about *what was sent*: the second attempt
/// puts identical bytes in front of an endpoint that already refused them.
/// Retrying that forever is worse than stopping, because the slot pins WAL for
/// as long as the connector keeps not making progress.
#[derive(Debug)]
pub(crate) enum SinkError {
    /// Feldera could not be reached, or reported a fault on its own side. The
    /// batch may or may not have landed. PostgreSQL has not been acknowledged
    /// either way, so replaying it is safe and the primary key absorbs the
    /// duplicate.
    Unavailable(String),

    /// Feldera understood the request and refused it.
    Rejected(String),
}

impl std::fmt::Display for SinkError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SinkError::Unavailable(reason) => {
                write!(formatter, "Feldera is unavailable: {reason}")
            }

            SinkError::Rejected(reason) => {
                write!(formatter, "Feldera refused the request: {reason}")
            }
        }
    }
}

impl std::error::Error for SinkError {}

impl From<reqwest::Error> for SinkError {
    fn from(error: reqwest::Error) -> Self {
        // A `reqwest::Error` surfacing here means the exchange never completed:
        // connection refused, DNS failure, timeout, a body that stopped
        // mid-transfer. None of those carry a verdict on the request's content.
        SinkError::Unavailable(error.to_string())
    }
}

/// Decide from the status code alone whether an unsuccessful response is worth
/// a retry.
///
/// Deliberately coarse. 5xx is Feldera stating the fault is on its side, and
/// 408 and 429 are the two 4xx codes that describe timing rather than content.
/// Everything else is treated as a verdict on the request, which errs towards
/// stopping: a connector that halts on a recoverable 404 is a page, while one
/// that retries an unsatisfiable request is silent WAL growth.
pub(crate) fn classify_status(status: reqwest::StatusCode, body: &str) -> SinkError {
    let message = format!("{status} - {body}");

    if status.is_server_error()
        || status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
    {
        SinkError::Unavailable(message)
    } else {
        SinkError::Rejected(message)
    }
}

pub(crate) struct FelderaSink {
    pub(crate) client: reqwest::Client,
    pub(crate) base: String,
    pub(crate) pipeline: String,
    pub(crate) pipeline_url: String,
    pub(crate) completion_status_url: String,
    pub(crate) query_url: String,
    pub(crate) ack_mode: AckMode,
    pub(crate) completion_timeout: Duration,
    pub(crate) require_primary_key: bool,
}

pub(crate) fn percent_encode_query_value(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());

    for byte in value.as_bytes() {
        match byte {
            // The unreserved set. Everything else is escaped, which is never
            // wrong, only occasionally more verbose than it needs to be.
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(*byte as char);
            }

            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }

    encoded
}

impl FelderaSink {
    pub(crate) fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let base = env_or("FELDERA_URL", "http://localhost:8080");
        let pipeline = env_or("FELDERA_PIPELINE", "JVM");

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

        // The escape hatch exists for the same reason AckMode::OnAccepted
        // does: the failure it prevents is worth being able to reproduce on
        // purpose rather than only describe.
        let require_primary_key = match env_or("FELDERA_REQUIRE_PRIMARY_KEY", "true").as_str() {
            "true" => true,
            "false" => false,
            other => {
                return Err(format!(
                    "FELDERA_REQUIRE_PRIMARY_KEY must be \"true\" or \"false\", got {other:?}"
                )
                .into());
            }
        };

        Ok(Self {
            client: reqwest::Client::new(),
            pipeline_url: format!("{base}/v0/pipelines/{pipeline}?selector=all"),
            completion_status_url: format!("{base}/v0/pipelines/{pipeline}/completion_status"),
            query_url: format!("{base}/v0/pipelines/{pipeline}/query"),
            base,
            pipeline,
            ack_mode,
            completion_timeout,
            require_primary_key,
        })
    }

    pub(crate) fn ingress_url(&self, table: &str) -> String {
        format!(
            "{}/v0/pipelines/{}/ingress/{table}\
             ?format=json&update_format=insert_delete&array=false",
            self.base, self.pipeline
        )
    }

    /// Read the destination table's declared key out of Feldera's compiled
    /// schema.
    ///
    /// This runs before the replication stream opens so that a table which
    /// cannot absorb a redelivery is refused while the connector is still
    /// harmless, rather than after it has started moving rows.
    pub(crate) async fn fetch_target_contract(
        &self,
        table: &str,
    ) -> Result<TargetContract, Box<dyn std::error::Error>> {
        let response = self.client.get(&self.pipeline_url).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await?;

            return Err(format!("Could not read the Feldera pipeline: {status} - {body}").into());
        }

        let pipeline: serde_json::Value = response.json().await?;

        let program_info = pipeline
            .get("program_info")
            .filter(|info| !info.is_null())
            .ok_or(
                "Feldera reports no compiled program for this pipeline, so its schema cannot be \
                 checked. Compile the pipeline before starting the connector.",
            )?;

        let inputs = program_info
            .pointer("/schema/inputs")
            .and_then(|inputs| inputs.as_array())
            .ok_or("Feldera's compiled program carries no input table schema")?;

        let relation = inputs
            .iter()
            .find(|input| {
                SqlIdentifier::from_json(input).is_some_and(|identifier| identifier.matches(table))
            })
            .ok_or_else(|| {
                let known: Vec<&str> = inputs
                    .iter()
                    .filter_map(|input| input.get("name")?.as_str())
                    .collect();

                format!(
                    "Feldera has no input table named {table:?}. It declares: {}",
                    known.join(", ")
                )
            })?;

        let table = SqlIdentifier::from_json(relation)
            .ok_or("Feldera returned an input table without a usable name")?;

        let fields: Vec<SqlIdentifier> = relation
            .get("fields")
            .and_then(|fields| fields.as_array())
            .map(|fields| fields.iter().filter_map(SqlIdentifier::from_json).collect())
            .unwrap_or_default();

        // `primary_key` lists bare column names, so whether each one is
        // case sensitive has to be recovered from the field of the same name.
        let primary_key = relation
            .get("primary_key")
            .and_then(|key| key.as_array())
            .map(|names| {
                names
                    .iter()
                    .filter_map(|name| name.as_str())
                    .map(|name| {
                        fields
                            .iter()
                            .find(|field| field.name == name)
                            .cloned()
                            .unwrap_or_else(|| SqlIdentifier {
                                name: name.to_string(),
                                case_sensitive: false,
                            })
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(TargetContract {
            table,
            primary_key,
            fields,
        })
    }

    /// Submit one batch. Returning `Ok` means Feldera accepted it, nothing more.
    pub(crate) async fn send(
        &self,
        ingress_url: &str,
        events: &[serde_json::Value],
    ) -> Result<Barrier, SinkError> {
        let response = self
            .client
            .post(ingress_url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(encode_change_events(events))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await?;

            return Err(classify_status(status, &body));
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
                        SinkError::Rejected(format!(
                            "accepted the batch but returned no completion token: {payload}"
                        ))
                    })?;

                Ok(Barrier::AwaitingCompletion {
                    token: token.to_string(),
                })
            }
        }
    }

    /// The rows Feldera currently holds, as delete events.
    ///
    /// The connector does not keep a copy of the table — Phase 7 is why — so
    /// the live key set is asked for at the moment it is needed. A snapshot is
    /// a replacement of that set, and a TRUNCATE is the same retract without
    /// the reload. Both are wrong if they only insert.
    ///
    /// The query names the primary key when there is one, because that is all
    /// a Feldera delete needs to match. Without a key it names every column,
    /// because a Z-set delete matches the whole record.
    pub(crate) async fn live_delete_events(
        &self,
        contract: &TargetContract,
    ) -> Result<Vec<serde_json::Value>, SinkError> {
        let columns = if contract.primary_key.is_empty() {
            contract.fields.as_slice()
        } else {
            contract.primary_key.as_slice()
        };

        if columns.is_empty() {
            return Err(SinkError::Rejected(
                "cannot retract Feldera table: it declares no columns to select".into(),
            ));
        }

        let select = columns
            .iter()
            .map(|column| quote_ident(&column.name))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!("SELECT {select} FROM {}", quote_ident(&contract.table.name));

        let url = format!(
            "{}?sql={}&format=json",
            self.query_url,
            percent_encode_query_value(&sql)
        );

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await?;

            return Err(classify_status(
                status,
                &format!(
                    "{body}. The destination table must be materialized so a snapshot or \
                     TRUNCATE can read the live rows to retract."
                ),
            ));
        }

        let body = response.text().await?;

        Ok(parse_query_rows(&body)?
            .into_iter()
            .map(|row| serde_json::json!({ "delete": row }))
            .collect())
    }

    /// Block until the batch behind `barrier` is safe to acknowledge, and report
    /// how long that took.
    pub(crate) async fn wait(&self, barrier: Barrier) -> Result<Duration, SinkError> {
        let started = Instant::now();

        let token = match barrier {
            Barrier::AlreadyResolved => return Ok(Duration::ZERO),
            Barrier::AwaitingCompletion { token } => token,
        };

        let status_url = format!(
            "{}?token={}",
            self.completion_status_url,
            percent_encode_query_value(&token)
        );

        let mut delay = COMPLETION_POLL_MIN;

        loop {
            let response = self.client.get(&status_url).send().await?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await?;

                return Err(classify_status(status, &body));
            }

            let payload: serde_json::Value = response.json().await?;

            match payload.get("status").and_then(|status| status.as_str()) {
                Some("complete") => return Ok(started.elapsed()),

                Some("inprogress") => {}

                Some(other) => {
                    return Err(SinkError::Rejected(format!(
                        "reported unknown status {other:?}"
                    )));
                }

                None => {
                    return Err(SinkError::Rejected(format!(
                        "completion check returned no status: {payload}"
                    )));
                }
            }

            if started.elapsed() >= self.completion_timeout {
                // Retryable, not fatal. A pipeline this slow may simply be
                // behind, and the reconnect loop's give-up deadline is what
                // bounds how long the connector is willing to wait it out.
                return Err(SinkError::Unavailable(format!(
                    "did not report completion within {:?}",
                    self.completion_timeout
                )));
            }

            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(COMPLETION_POLL_MAX);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_body_accepts_ndjson() {
        let rows = parse_query_rows("{\"id\":1}\n{\"id\":2}\n").unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], 1);
    }

    #[test]
    fn query_body_accepts_a_json_array() {
        let rows = parse_query_rows("[{\"id\":1},{\"id\":2}]").unwrap();

        assert_eq!(rows.len(), 2);
    }
}
