# PostgreSQL CDC Connector

A PostgreSQL logical replication connector written in Rust that streams database changes into Feldera.

The project started as a simple CDC prototype and evolved into a connector focused on correctness, recovery, bounded memory, backpressure, and reliable delivery.

## Architecture

```text
PostgreSQL
    │
    │ Logical Replication
    ▼
Rust Connector
    │
    ├── Decode
    ├── Schema Mapping
    ├── Transaction Tracking
    ├── Snapshot & Recovery
    ├── Bounded Outbox
    └── LSN & Acknowledgement
    │
    ▼
Feldera
```

PostgreSQL provides the change stream. The connector decodes and transforms the changes, routes them to the correct Feldera ingress, waits for durability, and only then advances the replication position.

## What it handles

The connector supports PostgreSQL logical replication with schema aware decoding and multiple table routing.

It handles INSERT, UPDATE, DELETE, TRUNCATE, transaction boundaries, relation metadata, replica identity, TOAST values, multiple PostgreSQL data types, initial snapshots, snapshot recovery, batching, backpressure, reconnects, graceful shutdown, retries, and acknowledgement based on sink durability.

## Design

### Snapshot consistency

A replication slot tracks WAL position, but it does not tell the connector whether an initial snapshot completed successfully.

The connector therefore keeps a small durable snapshot state and treats a snapshot as a replacement of the target state.

Before loading a replacement snapshot, it asks Feldera for the rows it currently holds and retracts them. This prevents stale rows from surviving a failed snapshot.

### Transaction boundaries

PostgreSQL transactions have no fixed size limit, so buffering an entire transaction can create an unbounded memory problem.

The connector uses a bounded outbox instead. Batches are flushed when they reach the configured size or when the oldest buffered event reaches the configured linger time.

Normal transactions retain their boundaries where possible. Oversized transactions may be delivered in multiple batches to keep memory bounded.

### Durability and acknowledgement

Receiving an event is not the same as safely delivering it.

The connector only advances the PostgreSQL replication position after the corresponding data has been accepted and confirmed durable by Feldera.

Anything that was not acknowledged can therefore be replayed safely after a crash.

### Backpressure

The connector waits when Feldera cannot accept or durably process data instead of allowing an unbounded in memory queue to grow.

This naturally applies backpressure to the PostgreSQL replication stream.

### Multi table routing

A publication can contain multiple tables. PostgreSQL relation IDs identify which table produced each change, and the connector maps those relations to their corresponding Feldera ingress.

This prevents changes from being written to the wrong table.

### TRUNCATE and reconciliation

TRUNCATE is handled as a target state retraction rather than as individual row deletes.

The same reconciliation mechanism is used when replacing an existing target during a new snapshot.

### Failure handling

The connector is tested around crashes, snapshot recovery, reconnects, acknowledgement boundaries, backpressure, oversized transactions, and restart behaviour.

The recovery model relies on replaying anything that was not safely acknowledged rather than trusting volatile process state.

## Configuration

Configuration is provided through environment variables for PostgreSQL, the replication slot, publication, tables, Feldera, snapshot behaviour, batching, and reconnect timeouts.

A single table can be configured directly:

```text
TABLES=users
```

Multiple tables can be configured:

```text
TABLES=users,items
```

PostgreSQL and Feldera names can also be mapped explicitly:

```text
TABLES=users:USERS,public.items:LINEITEMS
```

## Project structure

```text
src/
    main.rs
    config.rs
    connector.rs
    contract.rs
    decode.rs
    feldera.rs
    outbox.rs
    runtime.rs
    snapshot.rs
```

Each module owns a specific part of the connector rather than keeping the entire system inside main.rs.

## Running

PostgreSQL must have logical replication enabled with the required publication and replication slot configured.

Configure the required environment variables and start the connector.

Build and test with:

```sh
cargo build
cargo test
```

## Why this project

The interesting part of CDC is not making an INSERT appear in another system.

The difficult problems appear when transactions become huge, the sink becomes slow, the connector crashes during a snapshot, the replication connection disappears, the target already contains stale state, or PostgreSQL represents an operation such as TRUNCATE differently from ordinary row changes.

This project focuses on understanding and solving those problems at the connector boundary.