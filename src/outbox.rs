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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::{Row, Value};

    fn event() -> serde_json::Value {
        serde_json::json!({ "insert": { "id": 1 } })
    }

    fn contract() -> TargetContract {
        TargetContract {
            table: crate::contract::SqlIdentifier {
                name: "USERS".into(),
                case_sensitive: false,
            },
            primary_key: Vec::new(),
            fields: Vec::new(),
        }
    }

    // Invariant 6: the outbox stays bounded and flushes when due.

    #[test]
    fn an_empty_outbox_is_due() {
        assert!(Outbox::new().due(100, Duration::from_secs(60)));
    }

    #[test]
    fn outbox_is_due_on_the_size_bound() {
        let mut outbox = Outbox::new();
        outbox.push(0, vec![event(), event(), event(), event(), event()]);

        assert!(outbox.due(5, Duration::from_secs(60)));
        assert!(!outbox.due(6, Duration::from_secs(60)));
    }

    #[test]
    fn zero_linger_makes_a_nonempty_outbox_due() {
        let mut outbox = Outbox::new();
        outbox.push(0, vec![event()]);

        assert!(outbox.due(100, Duration::ZERO));
    }

    #[test]
    fn grouped_preserves_first_appearance_order() {
        let mut outbox = Outbox::new();
        outbox.push(1, vec![event(), event()]);
        outbox.push(0, vec![event()]);
        outbox.push(1, vec![event()]);

        let grouped = outbox.grouped();

        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[0].0, 1);
        assert_eq!(grouped[0].1.len(), 3);
        assert_eq!(grouped[1].0, 0);
        assert_eq!(grouped[1].1.len(), 1);
    }

    #[test]
    fn clearing_resets_the_outbox() {
        let mut outbox = Outbox::new();
        outbox.push(0, vec![event()]);
        outbox.clear();

        assert!(outbox.events.is_empty());
        assert!(outbox.oldest.is_none());
        assert!(outbox.due(100, Duration::from_secs(60)));
    }

    // A change must arrive inside a BEGIN..COMMIT, as pgoutput presents it.

    #[test]
    fn change_outside_a_transaction_is_refused() {
        let mut outbox = Outbox::new();
        let event = CdcEvent::Insert {
            row: Row {
                values: vec![("id".into(), Value::Int64(1))],
            },
        };

        let error = buffer_change(&mut outbox, None, "INSERT", &event, 0, &contract()).unwrap_err();

        assert!(error.to_string().contains("outside a transaction"));
    }

    #[test]
    fn buffered_change_counts_into_the_open_transaction() {
        let mut outbox = Outbox::new();
        let mut transaction = Transaction {
            xid: 7,
            changes: 0,
            events: 0,
        };
        let event = CdcEvent::Insert {
            row: Row {
                values: vec![("id".into(), Value::Int64(1))],
            },
        };

        buffer_change(
            &mut outbox,
            Some(&mut transaction),
            "INSERT",
            &event,
            0,
            &contract(),
        )
        .unwrap();

        assert_eq!(transaction.changes, 1);
        assert_eq!(transaction.events, 1);
        assert_eq!(outbox.events.len(), 1);
    }
}
