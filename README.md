# PostgreSQL CDC Connector

A PostgreSQL logical replication connector written in Rust that streams database changes into Feldera, focused on correctness, recovery, bounded memory, backpressure, and reliable delivery.

## Architecture

```text
PostgreSQL                                Feldera
    │                                         ▲
    │ logical replication (pgoutput)          │ INSERT/DELETE NDJSON + completion tokens
    ▼                                         │
┌──────────────────── Rust Connector ─────────────────┐
│                                                      │
│  session ─ decode ─ contract ─ outbox ─ sink        │
│  (stream loop)  (pgoutput/  (key + route (bounded,  (ingress,
│                  COPY, TOAST, checks)    size/linger  durability
│                  type coercion)          flush)       barrier)
│                                                      │
│  slot LSN only advances AFTER Feldera reports       │
│  the batch durable (at-least-once)                  │
│                                                      │
└──────────────────────────────────────────────────────┘

PostgreSQL provides the change stream. The connector decodes and transforms
changes, routes them to the right Feldera ingress, waits for durability, and
only then advances the replication position.
```

## What it handles

INSERT, UPDATE, DELETE, TRUNCATE, transaction boundaries, relation metadata, replica identity, TOAST values, multiple PostgreSQL data types, initial snapshots and recovery, batching, backpressure, reconnects, graceful shutdown, retries, and durability-based acknowledgement.

## Design

- **At-least-once delivery** — the position only advances after Feldera confirms a batch durable; anything unacknowledged is replayed after a crash. Safe only because the target declares a PRIMARY KEY (without one, a replayed insert leaves a row at weight 2 and it survives forever).
- **Safety contract** — each Feldera table's key is checked at startup, and each source table must carry a REPLICA IDENTITY that covers it, so deletes can always be keyed. An unkeyed delete is silently ignored by Feldera, so it is refused rather than sent.
- **Snapshot as replacement** — the slot is created first and exports a snapshot, the table is read inside it via `SET TRANSACTION SNAPSHOT`, and the stream resumes at that exact LSN: one consistent point. Progress lives in `cdc-state.json` (`SNAPSHOT_STATE_PATH`). Because a snapshot is a replacement, Feldera's current rows are retracted before the COPY — a failed snapshot can't leave stale rows. On restart, the state file and slot existence decide between resume and fresh snapshot.
- **Toasted columns** — an UPDATE that doesn't rewrite a TOASTed column arrives as a hole (`UnchangedToast`). It is filled from a FULL old image or the change is refused; a placeholder is never sent.
- **Bounded outbox** — batches flush on size (`BATCH_MAX_ROWS`) or linger (`BATCH_LINGER_MS`); oversized transactions go out in pieces instead of growing memory without bound.
- **Backpressure** — when Feldera is slow, the connector waits (honestly reporting the old position in keepalives) instead of queuing without bound. This pushes back on PostgreSQL naturally.
- **TRUNCATE** — handled as a target-state retraction: unsent buffered events are dropped (the source already discarded those rows), then Feldera's live rows are deleted.
- **LSN & acknowledgement** — position tracking can never go backwards; the status interval is derived from `wal_sender_timeout`; on shutdown the final LSN is confirmed by re-reading the slot, since a status update has no reply in the protocol.
- **Fault injection** — `CRASH_AT` kills the process at five points (`during-snapshot`, `before-send`, `after-send`, `after-durable`, `after-ack`) to prove every recovery path.

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

### Snapshot

| Variable | Default | Purpose |
| --- | --- | --- |
| `SNAPSHOT_MODE` | `initial` | `initial`, `never`, or `inconsistent` (the latter only to demonstrate loss). |
| `SNAPSHOT_STATE_PATH` | `./cdc-state.json` | Durable snapshot state file. |
| `SNAPSHOT_BATCH_ROWS` | `1000` | Rows per snapshot request. |

### Feldera

| Variable | Default | Purpose |
| --- | --- | --- |
| `FELDERA_URL` | `http://localhost:8080` | Feldera base URL. |
| `FELDERA_PIPELINE` | `JVM` | Pipeline name. |
| `FELDERA_ACK_MODE` | `completed` | `completed` (safe) or `accepted` (lossy, demonstrative only). |
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
| `CRASH_AT` | unset | `during-snapshot`, `before-send`, `after-send`, `after-durable`, or `after-ack`. |
| `CRASH_AFTER_BATCHES` | `1` | Which batch to crash on. |

## Project structure

```text
src/
    main.rs       supervision: contracts, snapshot gating, reconnect/backoff, shutdown, crash points
    config.rs     shared env access
    connector.rs  the replication session: stream loop, flush, durability wait, retraction
    contract.rs   Feldera ↔ PostgreSQL contract: routing, keyed-delete enforcement, identity checks
    decode.rs     pgoutput + COPY decoding, TOAST-hole filling, type coercion, JSON conversion
    feldera.rs    HTTP sink: ingress, completion tokens, live-row queries, error classification
    outbox.rs     bounded per-route buffer, size/linger/commit flush decisions
    runtime.rs    positions, standby status, reconnect bounds, slot confirmation, crash points
    snapshot.rs   slot creation, exported snapshot, state file, startup decisions, COPY
```

## Running

Prerequisites on PostgreSQL:

```sql
CREATE PUBLICATION my_pub FOR TABLE users WITH (publish = 'insert, update, delete, truncate');
ALTER TABLE users REPLICA IDENTITY FULL;  -- or a primary key covering Feldera's key
```

Slots are created by the connector. The publication must carry `truncate`, or TRUNCATE silently strands rows in Feldera. On Feldera: pipeline compiled and running, destination table materialized with a PRIMARY KEY.

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

Tests and validation: 53+ unit tests covering the invariants, a live harness (`scripts/harness.py`) with a crash-point matrix, randomized fuzzer, smoke test, and benchmarks. See `docs/`.

## Why this project

The interesting part of CDC is not making an INSERT appear in another system. The difficult problems appear when transactions become huge, the sink becomes slow, the connector crashes during a snapshot, the replication connection disappears, the target already contains stale state, or PostgreSQL represents an operation such as TRUNCATE differently from ordinary row changes. This project focuses on understanding and solving those problems at the connector boundary.