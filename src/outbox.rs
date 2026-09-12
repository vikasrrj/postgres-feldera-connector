//! Converted events waiting to be sent, grouped by Feldera table.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::contract::TargetContract;
use crate::feldera::{CdcEvent, feldera_change_events};

/// What is known about the transaction currently open on the stream.
///
/// Counters rather than the changes themselves. Holding the changes was the
/// obvious design and it is the one that dies: a transaction on the source has
/// no size limit, so a buffer that spans one has no size limit either, and the
/// process is killed by a workload that PostgreSQL handled without complaint.
/// The changes now go straight into the outbox, which is bounded; what survives
/// here is only what the logging needs.
#[derive(Debug)]
pub(crate) struct Transaction {
    pub(crate) xid: u32,
    pub(crate) changes: u64,
    pub(crate) events: u64,
}

/// One converted change, already aimed at a Feldera ingress.
///
/// Feldera's HTTP API is one table per URL. A transaction that touches two
/// PostgreSQL tables therefore cannot go out as one request, and the route
/// has to travel with the event from the moment it is converted — waiting
/// until flush and guessing the table from the payload is how rows from
/// `items` land in `users`.
pub(crate) struct OutboxEvent {
    pub(crate) route: usize,
    pub(crate) payload: serde_json::Value,
}

pub(crate) struct Outbox {
    pub(crate) events: Vec<OutboxEvent>,
    /// End LSN of the most recent COMMIT whose events are *entirely* in here,
    /// or were flushed from here and made durable. Zero until the first commit
    /// of the session, which is why a first transaction bigger than the bound
    /// acknowledges nothing until it finishes.
    pub(crate) committed_through: u64,
    /// When the oldest event now buffered was added, which is what the linger
    /// deadline is measured from. `None` exactly when `events` is empty.
    ///
    /// Measured from the oldest rather than the newest deliberately. A steady
    /// stream of arrivals would keep pushing a newest-based deadline out of
    /// reach and the batch would never be sent, which is the classic way a
    /// batching timer turns into an unbounded delay.
    pub(crate) oldest: Option<tokio::time::Instant>,
}

impl Outbox {
    pub(crate) fn new() -> Self {
        Self {
            events: Vec::new(),
            committed_through: 0,
            oldest: None,
        }
    }

    pub(crate) fn push(&mut self, route: usize, events: Vec<serde_json::Value>) {
        if self.oldest.is_none() {
            self.oldest = Some(tokio::time::Instant::now());
        }

        self.events.extend(
            events
                .into_iter()
                .map(|payload| OutboxEvent { route, payload }),
        );
    }

    pub(crate) fn clear(&mut self) {
        self.events.clear();
        self.oldest = None;
    }

    /// The instant at which the buffered events stop waiting for company.
    ///
    /// Absolute rather than a remaining duration, which is what makes it safe
    /// to build a fresh `sleep_until` on every loop iteration: the deadline
    /// does not move when the future is dropped and recreated, so the timer
    /// cannot be reset by unrelated traffic arriving.
    pub(crate) fn deadline(&self, linger: Duration) -> tokio::time::Instant {
        self.oldest.unwrap_or_else(tokio::time::Instant::now) + linger
    }

    /// Whether this batch should go out now.
    ///
    /// Empty counts as due. There is no request to send, but there may be a
    /// position to acknowledge — a transaction that touched only unpublished
    /// tables produces no events, and making it wait would stall the slot for
    /// no benefit. A zero linger also lands here, because the deadline is then
    /// the moment the first event arrived, which is already past.
    pub(crate) fn due(&self, max_rows: usize, linger: Duration) -> bool {
        self.events.is_empty()
            || self.events.len() >= max_rows
            || self.deadline(linger) <= tokio::time::Instant::now()
    }

    /// Events grouped by Feldera table, in the order those tables first appeared.
    ///
    /// One request per table, not one request for the whole buffer: the ingress
    /// URL names the table, and a mixed body would file every row under
    /// whichever table the first event happened to belong to.
    pub(crate) fn grouped(&self) -> Vec<(usize, Vec<serde_json::Value>)> {
        let mut order = Vec::new();
        let mut groups: BTreeMap<usize, Vec<serde_json::Value>> = BTreeMap::new();

        for event in &self.events {
            if !groups.contains_key(&event.route) {
                order.push(event.route);
            }

            groups
                .entry(event.route)
                .or_default()
                .push(event.payload.clone());
        }

        order
            .into_iter()
            .map(|route| (route, groups.remove(&route).unwrap_or_default()))
            .collect()
    }
}

/// Convert one change and put it in the outbox.
///
/// Converting here rather than at COMMIT is the difference between buffering a
/// transaction and buffering a batch. The `CdcEvent` exists for the length of
/// this call and is dropped on the way out, so what the connector holds for a
/// long-running transaction is the JSON it is going to send and nothing else.
pub(crate) fn buffer_change(
    outbox: &mut Outbox,
    transaction: Option<&mut Transaction>,
    kind: &str,
    event: &CdcEvent,
    route: usize,
    contract: &TargetContract,
) -> Result<(), Box<dyn std::error::Error>> {
    // A change outside a transaction is not a recoverable hiccup: pgoutput
    // frames every change between BEGIN and COMMIT, so seeing one loose means
    // the stream was misread, and guessing which transaction it belonged to is
    // how a connector invents a commit boundary that never existed.
    let transaction =
        transaction.ok_or_else(|| format!("{kind} received outside a transaction"))?;

    let events = feldera_change_events(event, contract)?;

    transaction.changes += 1;
    transaction.events += events.len() as u64;

    outbox.push(route, events);

    Ok(())
}
