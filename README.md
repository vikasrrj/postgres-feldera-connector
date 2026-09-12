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

### Safety contract

The connector delivers at least once: after a crash it replays everything that was not acknowledged. That is only harmless if Feldera overwrites a row it already holds instead of counting it twice, which requires the destination table to declare a PRIMARY KEY.

At startup, each Feldera table's key is read from the pipeline's compiled schema. A table without a key is refused unless `FELDERA_REQUIRE_PRIMARY_KEY=false`, which is only useful for demonstrating why the key is required.

Every source table must also carry a REPLICA IDENTITY that covers Feldera's key, so PostgreSQL logs the columns a delete needs to match. A table with `REPLICA IDENTITY NOTHING` (or no usable identity) is refused; a partial gap is warned about and enforced at the point where an unkeyed delete would actually be sent. Fix with `ALTER TABLE ... REPLICA IDENTITY FULL;` or a covering identity index.

### Snapshot consistency

A replication slot tracks WAL position, but it does not tell the connector whether an initial snapshot completed successfully.

A snapshot is taken in three ordered steps: the slot is created first and exports the snapshot it started from, the table is read inside that exported snapshot, and the stream then resumes at exactly that LSN. Every row is delivered by exactly one of the two paths: committed at or before the consistent point it is in the COPY but not replayed by the stream, committed after it the stream carries it. There is no window in which a row belongs to neither.

The connector keeps a small durable state file (`cdc-state.json` by default) recording which slot, its consistent point, and whether the snapshot finished. A crash mid-snapshot leaves the file saying `pending`, so the next run reads the table again instead of trusting a partially loaded target.

A snapshot is a replacement, not an update. Before loading one, the connector asks Feldera for the rows it currently holds and retracts them, so stale rows never survive a failed snapshot.

Snapshot behaviour is chosen with `SNAPSHOT_MODE`:

- `initial` (default): slot + exported snapshot + stream, one consistent point.
- `never`: stream only, no state file is read or written. Correct when the target is already known to hold the table. A slot that does not exist is created and streaming starts from now, leaving pre-existing rows out of the target.
- `inconsistent`: read the table before creating the slot. Loses every row committed between the two moments. Exists to demonstrate that loss, not to be run for real.

### Transaction boundaries

PostgreSQL transactions have no fixed size limit, so buffering an entire transaction can create an unbounded memory problem.

The connector uses a bounded outbox instead. Batches are flushed when they reach the configured size or when the oldest buffered event reaches the configured linger time.

Normal transactions retain their boundaries where possible. Oversized transactions may be delivered in multiple batches to keep memory bounded.

### Durability and acknowledgement

Receiving an event is not the same as safely delivering it.

The connector only advances the PostgreSQL replication position after the corresponding data has been accepted and confirmed durable by Feldera.

By default `FELDERA_ACK_MODE=completed` waits for Feldera's completion token, which means every attached sink wrote the batch. `FELDERA_ACK_MODE=accepted` acknowledges as soon as Feldera buffers the request and exists to demonstrate that a pipeline crash then loses data.

Anything that was not acknowledged can therefore be replayed safely after a crash.

### Backpressure

The connector waits when Feldera cannot accept or durably process data instead of allowing an unbounded in memory queue to grow.

This naturally applies backpressure to the PostgreSQL replication stream.

### Multi table routing

A publication can contain multiple tables. PostgreSQL relation IDs identify which table produced each change, and the connector maps those relations to their corresponding Feldera ingress.

This prevents changes from being written to the wrong table. Rows that arrive for a table the connector is not configured for stop the session rather than being filed under a wrong name.

### TRUNCATE and reconciliation

TRUNCATE is handled as a target state retraction rather than as individual row deletes.

The same reconciliation mechanism is used when replacing an existing target during a new snapshot.

TRUNCATE is a publication option, not a table option. A publication created with `publish = 'insert, update, delete'` will never emit it, and the connector would then look caught up while Feldera still held every row. The connector warns on start when `pubtruncate` is false, and recommends `ALTER PUBLICATION ... SET (publish = 'insert, update, delete, truncate');`.

### TOAST

A column updated without being rewritten is sent by pgoutput as a marker instead of a value. Under `REPLICA IDENTITY FULL` the old tuple carries the real value and the new row is completed from it. Under any thinner identity the connector refuses the update rather than silently losing the column; the fix is `ALTER TABLE ... REPLICA IDENTITY FULL;`.

### Types and normalisation

Snapshot COPY and the replication stream are decoded by the same value decoder, so a value means the same thing on first load and on replay.

- `timestamptz` is shifted to UTC, because the snapshot connection and the walsender may use different session time zones.
- `numeric` is kept as exact text, since JSON has no decimal type.
- `bytea` is encoded as a JSON array of bytes, matching Feldera's own CDC output.
- `NaN` and infinity for floats map to JSON null.

Unsupported type OIDs are refused with a clear error rather than guessed at.

### Liveness

The status update interval is derived from the server's own `wal_sender_timeout` rather than hardcoded, so a report always lands well inside the server's patience and the connector never appears dead on an idle database. A disabled timeout falls back to a 10 second ceiling.

### Failure handling

The connector is tested around crashes, snapshot recovery, reconnects, acknowledgement boundaries, backpressure, oversized transactions, and restart behaviour.

The recovery model relies on replaying anything that was not safely acknowledged rather than trusting volatile process state.

Reconnects back off with full jitter and give up after `RECONNECT_GIVE_UP_SECS` of no progress, exiting nonzero instead of holding WAL on the source forever.

On graceful shutdown the connector sends a final standby status update and then re-reads the slot over a second connection to confirm PostgreSQL actually recorded it, because a standby status update is one-way and has no acknowledgement. If the LSN never lands, the transactions after it will be redelivered.

Fault injection is available for testing these boundaries: `CRASH_AT` chooses a point (one of `during-snapshot`, `before-send`, `after-send`, `after-durable`, `after-ack`) and `CRASH_AFTER_BATCHES` says which batch to die on. Each point sits on a boundary between the steps whose ordering the design rests on.

## Configuration

Configuration is provided through environment variables.

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

The table must have a usable REPLICA IDENTITY (a primary key or `REPLICA IDENTITY FULL`) so deletes can be keyed. The slots are created by the connector.

Prerequisites on Feldera:

The pipeline must be compiled and running, the destination table must be materialized (so the live rows can be read for retraction) and have a PRIMARY KEY that matches the connector's expectations.

Configure the required environment variables and start the connector:

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

The interesting part of CDC is not making an INSERT appear in another system.

The difficult problems appear when transactions become huge, the sink becomes slow, the connector crashes during a snapshot, the replication connection disappears, the target already contains stale state, or PostgreSQL represents an operation such as TRUNCATE differently from ordinary row changes.

This project focuses on understanding and solving those problems at the connector boundary.