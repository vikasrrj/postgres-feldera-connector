# PostgreSQL CDC Connector

Streams PostgreSQL logical replication changes into Feldera, focused on correctness, recovery, bounded memory, backpressure, and reliable delivery.

## Architecture

```text
PostgreSQL
    │ Logical Replication
    ▼
Rust Connector
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

The connector decodes and transforms changes, routes them to the right Feldera ingress, waits for durability, and only then advances the replication position.

## What it handles

INSERT, UPDATE, DELETE, TRUNCATE, transaction boundaries, relation metadata, replica identity, TOAST values, multiple PostgreSQL data types, initial snapshots and recovery, batching, backpressure, reconnects, graceful shutdown, retries, and durability-based acknowledgement.

## Design

- **At-least-once delivery** — the position only advances after Feldera confirms a batch durable; anything unacknowledged is replayed after a crash. Safe only because destinations declare a PRIMARY KEY.
- **Safety contract** — each Feldera table's key is checked at startup, and each source table must carry a REPLICA IDENTITY that covers it, so deletes can always be keyed.
- **Snapshot** — the slot is created first and exports a snapshot, the table is read inside it, and the stream resumes at that exact LSN: one consistent point. Progress lives in `cdc-state.json` (`SNAPSHOT_STATE_PATH`). A snapshot is a replacement, so Feldera's current rows are retracted before the COPY.
- **Bounded outbox** — batches flush on size (`BATCH_MAX_ROWS`) or linger (`BATCH_LINGER_MS`); oversized transactions go out in pieces instead of growing memory without bound.
- **LSN & acknowledgement** — the status interval is derived from `wal_sender_timeout`, the slot is advertised only on durable batches, TRUNCATE retracts the whole target, and on shutdown the final LSN is confirmed by re-reading the slot.

## Configuration

A single table:

```text
TABLES=users
```

Multiple tables, with optional name mapping:

```text
TABLES=users:USERS,public.items:LINEITEMS
```

`PG_TABLE` / `FELDERA_TABLE` / `PG_SCHEMA` configure a single table instead of `TABLES`.

### PostgreSQL

| Variable | Default | Purpose |
| --- | --- | --- |
| `POSTGRES_URL` | `host=localhost user=postgres password=postgres` | Base connection string; `replication=database` is appended for the replication connection. |
| `PG_SLOT` | `rust_slot` | Logical replication slot name. |
| `PG_PUBLICATION` | `my_pub` | Publication to read. |

### Tables

| Variable | Default | Purpose |
| --- | --- | --- |
| `TABLES` | — | Comma separated list of `source[:DESTINATION]` pairs. |
| `PG_TABLE` | value of `FELDERA_TABLE` | Source table for single-table mode. |
| `FELDERA_TABLE` | `users` | Destination table for single-table mode. |
| `PG_SCHEMA` | `public` | Schema for unqualified table names. |

### Snapshot

| Variable | Default | Purpose |
| --- | --- | --- |
| `SNAPSHOT_MODE` | `initial` | `initial`, `never`, or `inconsistent`. |
| `SNAPSHOT_STATE_PATH` | `./cdc-state.json` | Durable snapshot state file. |
| `SNAPSHOT_BATCH_ROWS` | `1000` | Rows per snapshot request. |

### Feldera

| Variable | Default | Purpose |
| --- | --- | --- |
| `FELDERA_URL` | `http://localhost:8080` | Feldera base URL. |
| `FELDERA_PIPELINE` | `JVM` | Pipeline name. |
| `FELDERA_ACK_MODE` | `completed` | `completed` or `accepted`. |
| `FELDERA_REQUIRE_PRIMARY_KEY` | `true` | Refuse keyless destination tables. |
| `FELDERA_COMPLETION_TIMEOUT_SECS` | `60` | How long to poll a completion token before treating the sink as unavailable. |

### Batching and reconnect

| Variable | Default | Purpose |
| --- | --- | --- |
| `BATCH_MAX_ROWS` | `10000` | Max events per request; a transaction larger than this is sent in pieces. |
| `BATCH_LINGER_MS` | `50` | How long a batch waits for company before being sent; `0` sends per transaction. |
| `RECONNECT_GIVE_UP_SECS` | `300` | Give up and exit nonzero after this long without replication progress. |

### Fault injection

| Variable | Default | Purpose |
| --- | --- | --- |
| `CRASH_AT` | unset | Crash at `during-snapshot`, `before-send`, `after-send`, `after-durable`, or `after-ack`. |
| `CRASH_AFTER_BATCHES` | `1` | Which batch to crash on. |

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

Prerequisites on PostgreSQL:

```sql
CREATE PUBLICATION my_pub FOR TABLE users WITH (publish = 'insert, update, delete, truncate');
```

The table needs a usable REPLICA IDENTITY (primary key or `REPLICA IDENTITY FULL`). Slots are created by the connector.

Prerequisites on Feldera: pipeline compiled and running, destination table materialized with a PRIMARY KEY.

```sh
export POSTGRES_URL="host=localhost user=postgres password=postgres"
export TABLES="users"
export FELDERA_URL="http://localhost:8080"
export FELDERA_PIPELINE="JVM"
cargo run
```

Build and test with:

```sh
cargo build
cargo test
```

## Why this project

The interesting part of CDC is not making an INSERT appear in another system. The difficult problems appear when transactions become huge, the sink becomes slow, the connector crashes during a snapshot, the replication connection disappears, the target already contains stale state, or PostgreSQL represents an operation such as TRUNCATE differently from ordinary row changes.