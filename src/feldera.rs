//! Feldera HTTP sink: ingress, completion tokens, ad-hoc query.

use std::time::{Duration, Instant};

use crate::config::env_or;
use crate::contract::{SqlIdentifier, TargetContract, check_delete_carries_key};
use crate::decode::{PreviousRow, Row, quote_ident, row_to_json};

/// One change to the replicated table, decoded and not yet translated.
#[derive(Debug)]
pub(crate) enum CdcEvent {
    Insert { row: Row },

    Update { old: PreviousRow, new_row: Row },

    Delete { old: PreviousRow },
}

// ============================================================
// FELDERA
// ============================================================

// Feldera ingress expects one JSON key per event, so an UPDATE is a delete + insert.
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

        // Retracting the old row and inserting the new one stays correct
        // whether or not the identity changed.
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

pub(crate) fn encode_change_events(events: &[serde_json::Value]) -> String {
    use std::fmt::Write;

    let mut encoded = String::new();

    for (index, event) in events.iter().enumerate() {
        if index > 0 {
            encoded.push('\n');
        }

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

// Feldera /query returns NDJSON, but some deployments wrap the same rows in [...].
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
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum AckMode {
    /// Treat acceptance as safety. Loses data if the pipeline dies with the
    /// buffer unprocessed.
    OnAccepted,
    /// Wait for Feldera to report the batch fully processed.
    OnCompleted,
}

/// Handed back by [`FelderaSink::send`]; owning one is not yet safety.
#[derive(Debug)]
pub(crate) enum Barrier {
    /// Nothing to wait for: this mode equates acceptance with safety.
    AlreadyResolved,
    /// Feldera issued a completion token, and safety means polling it.
    AwaitingCompletion { token: String },
}

const COMPLETION_POLL_MIN: Duration = Duration::from_millis(5);
const COMPLETION_POLL_MAX: Duration = Duration::from_millis(250);

/// A wait beyond this is worth reporting: it shows up as WAL pressure on PostgreSQL.
pub(crate) const SLOW_WAIT_WARNING: Duration = Duration::from_secs(5);

/// The most Feldera events one request will carry.
pub(crate) const BATCH_MAX_ROWS_DEFAULT: &str = "10000";

/// How long a batch waits for company before it is sent; zero disables it.
pub(crate) const BATCH_LINGER_MS_DEFAULT: &str = "50";

/// Whether a Feldera failure is worth retrying.
#[derive(Debug)]
pub(crate) enum SinkError {
    /// The batch may or may not have landed; replaying it is safe.
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
        // A reqwest::Error means the exchange never completed, so it carries
        // no verdict on the request's content.
        SinkError::Unavailable(error.to_string())
    }
}

/// Decide from the status code alone whether an unsuccessful response is worth
/// a retry.
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
            // The unreserved set; everything else is escaped.
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
    /// schema, before the replication stream opens.
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

        // primary_key lists bare column names, so case sensitivity is recovered
        // from the field of the same name.
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

                // A missing token must fail rather than fall back to treating
                // acceptance as safety.
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

    /// The rows Feldera currently holds, as delete events. Queried live because
    /// the connector does not keep a copy of the table.
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
                // Retryable, not fatal: a slow pipeline is behind, and the
                // give-up deadline bounds how long we wait it out.
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
