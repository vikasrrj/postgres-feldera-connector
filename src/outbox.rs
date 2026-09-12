//! Converted events waiting to be sent, grouped by Feldera table.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::contract::TargetContract;
use crate::feldera::{CdcEvent, feldera_change_events};

/// What is known about the transaction currently open on the stream.
#[derive(Debug)]
pub(crate) struct Transaction {
    pub(crate) xid: u32,
    pub(crate) changes: u64,
    pub(crate) events: u64,
}

/// One converted change, already aimed at a Feldera ingress.
pub(crate) struct OutboxEvent {
    pub(crate) route: usize,
    pub(crate) payload: serde_json::Value,
}

pub(crate) struct Outbox {
    pub(crate) events: Vec<OutboxEvent>,
    /// End LSN of the most recent commit whose events are all durable.
    pub(crate) committed_through: u64,
    /// When the oldest buffered event was added; None when the outbox is empty.
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

    pub(crate) fn deadline(&self, linger: Duration) -> tokio::time::Instant {
        self.oldest.unwrap_or_else(tokio::time::Instant::now) + linger
    }

    pub(crate) fn due(&self, max_rows: usize, linger: Duration) -> bool {
        self.events.is_empty()
            || self.events.len() >= max_rows
            || self.deadline(linger) <= tokio::time::Instant::now()
    }

    /// Events grouped by Feldera table, in the order those tables first appeared.
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

pub(crate) fn buffer_change(
    outbox: &mut Outbox,
    transaction: Option<&mut Transaction>,
    kind: &str,
    event: &CdcEvent,
    route: usize,
    contract: &TargetContract,
) -> Result<(), Box<dyn std::error::Error>> {
    // pgoutput frames every change between BEGIN and COMMIT, so one outside
    // a transaction means the stream was misread.
    let transaction =
        transaction.ok_or_else(|| format!("{kind} received outside a transaction"))?;

    let events = feldera_change_events(event, contract)?;

    transaction.changes += 1;
    transaction.events += events.len() as u64;

    outbox.push(route, events);

    Ok(())
}