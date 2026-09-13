# PostgreSQL CDC Connector

A PostgreSQL logical replication connector written in Rust that streams database changes into Feldera, focused on correctness, recovery, bounded memory, backpressure, and reliable delivery.

The project started as a CDC prototype and evolved into a connector whose central claims — *no data loss on crash*, *bounded memory under any transaction size*, *honest acknowledgements* — are enforced, refused, or loudly warned at every boundary rather than assumed.

## Why CDC is hard here

The interesting part of CDC is not making an INSERT appear in another system. The difficult problems appear when:

- transactions become huge enough to blow up an in-memory buffer,
- the sink becomes slow and must become backpressure instead of an unbounded queue,
- the connector crashes in the middle of a snapshot,
- the replication connection disappears and the WAL has to be replayed,
- the target already contains stale state left by a failed run,
- PostgreSQL represents operations (TOAST holes in UPDATE, TRUNCATE, unkeyed pre-images) in ways that look nothing like row changes, and
- the replication position must never advance past data the sink has not made safe.

This document walks through each layer of the connector and explains how it answers those problems. Everything below is grounded in the sources in `src/`.

## Architecture

```text
PostgreSQL                            Feldera
    │                                     ▲
    │ logical replication (pgoutput)      │ ingress: INSERT/DELETE NDJSON
    ▼                                     │ completion: /completion_status
┌────────────────────── Rust Connector ─────────────────────┐
│                                                            │
│  1. Replication session   (connector.rs run_session)       │
│     │  RELATION/BEGIN/INSERT/UPDATE/DELETE/COMMIT/TRUNCATE │
│     ▼                                                      │
│  2. Decode layer          (decode.rs)  byte → Value        │
│     │  TOAST-hole filling, type coercion, UTC shift        │
│     ▼                                                      │
│  3. Contract layer        (contract.rs) key routing,       │
│     │  keyed-delete enforcement, retraction planning       │
│     ▼                                                      │
│  4. Outbox                (outbox.rs) bounded, per-route   │
│     │  size / linger / commit-triggered flush              │
│     ▼                                                      │
│  5. Feldera sink          (feldera.rs) send + durability   │
│     │  barrier = completion token between accept & safe    │
│     ▼                                                      │
│  slot LSN only advances AFTER Feldera says "durable"       │
│     │                                                      │
│     └── standby status (received / flush) back to PG       │
│                                                            │
│  In-flight supervision: main.rs (snapshot first, then a    │
│  forever loop of sessions with backoff; crash points)      │
└────────────────────────────────────────────────────────────┘
```

Two data paths cross here:

- **Data flows down**: PostgreSQL → decode → contract → outbox → Feldera.
- **Acknowledgment flows up**: Feldera completion → advance `flush` LSN → standby status back to PostgreSQL.

The connector has no state of its own between the two except the bounded outbox and a durable snapshot state file — everything else is deliberately volatile and re-discoverable.

## Delivery contract

### At-least-once, gated on durability

Receiving an event is not the same as safely delivering it. The connector only advances the replication slot's flush LSN after the corresponding data has been accepted **and reported complete** by Feldera (`FELDERA_ACK_MODE=completed`, the default):

- `FelderaSink::send` returns a `Barrier` — *owning one is not safety* (`src/feldera.rs:166`). Safety means polling the completion token until it reports `complete`.
- `wait_for_durability` in `connector.rs` blocks on that token while still answering PostgreSQL keepalives — and those keepalives deliberately report the **old** flush position, because "a keepalive that lies is worse than none" (`src/connector.rs:646`).
- Anything never acknowledged is replayed after a crash. The slot is the source of truth, not the connector's memory.

The `accepted` mode treats Feldera's buffer being full as safety and exists only to demonstrate the data loss that produces; the connector refuses to ship silently, printing a warning at startup (`src/main.rs:64`).

### Why a PRIMARY KEY is mandatory

At least once plus crash replay means Feldera may apply the same insert twice. The key is what makes that idempotent: Feldera's primary-key aggregation *replaces* the row instead of counting occurrences. Without a key, a replayed insert leaves the row at weight 2, a later DELETE only brings it back to 1, and the row survives forever (a Z-set counts, it does not dedupe). Deleting an absent row from a keyless table is undefined in Feldera and can crash the pipeline. So a keyless destination is refused by default, and when forced (`FELDERA_REQUIRE_PRIMARY_KEY=false`) the connector prints exactly what divergence is now reachable (`src/main.rs:85`).

The *same* reasoning makes keyed deletes mandatory: Feldera silently ignores a delete that does not name a full key, so `check_delete_carries_key` refuses to emit one — an unkeyed delete is a permanent silent divergence (`src/contract.rs:307`).

## Layer 1 — Replication session

### Connecting

A base `POSTGRES_URL` is reused for ordinary queries (snapshot, catalog, slot management). The replication connection appends `replication=database` — without it `START_REPLICATION` fails later with a confusing error. The separator differs by connection-string form, which the code handles explicitly rather than concatenating blindly (`src/main.rs:28`):

- URL form (`postgresql://...`): a `?param` query, appended with `&` when one already exists.
- conninfo form (`host=... password=...`): a whitespace-delimited option.

Connections use TLS via `postgres-native-tls` when the string asks for it, and the background driver task is never awaited so it outlives the session's bindings.

### Slot creation and the exported snapshot

`CREATE_REPLICATION_SLOT <slot> LOGICAL pgoutput (SNAPSHOT 'export')` hands back a `consistent_point` (the LSN the slot starts at) and a snapshot name. Two subtleties matter:

- **`SNAPSHOT 'nothing'`** is used when the stream starts without reading the table. Exporting is the default behavior that pins `xmin` (blocks vacuum) for the life of the connection, so it is only asked for when it will actually be read (`src/connector.rs:127`).
- **An exported snapshot only exists while the replication connection that created it stays open.** That is why `create_slot` returns the connection rather than closing it, and why the snapshot's COPY must finish before the replication connection is dropped (`src/connector.rs:249`).

### Reading the stream

`START_REPLICATION SLOT <slot> LOGICAL 0/0` — a start LSN of `0/0` tells the server to resume from the slot's `confirmed_flush_lsn`, whatever the connector had acknowledged in an earlier life (`src/connector.rs:814`).

Per session, the connector rebuilds the state the stream implicitly resets on reconnect: the relation-metadata map (`RELATION` messages are re-sent on a new stream), the open transaction, and the outbox. Buffered-but-unsent events are simply dropped on disconnect — they were never acknowledged, so replay from the slot position redelivers them.

### LSN bookkeeping that cannot go backwards

The connector tracks two positions: `received` (latest WAL end seen) and `flush` (latest durable end). Both are **max'd**, never assigned, because:

- `wal_end` is `0` on some messages and assigning it directly would drag the position backwards (`src/connector.rs:921`),
- a keepalive fires with nothing new committed, so `flush` must not retreat to a smaller `committed_through`.

A PostgreSQL keepalive (which asserts nothing further is coming up to `wal_end`) only advances the position when **no transaction is open and no events are buffered** (`src/connector.rs:1250`). Even with an idle database the slot advances, so the oldest WAL is reclaimed rather than retained forever; but it never runs ahead of received-but-not-durable changes, because TCP delivers in order.

`wal_sender_timeout` is read from the *running* server at startup and the standby-status interval is derived from it (`timeout / 3`, clamped to `[500ms, 10s]`, `src/runtime.rs:242`). If the GUC is disabled the connector reports at the 10s ceiling anyway rather than letting a default assumption apply.

## Layer 2 — Decode layer

### pgoutput text tuples

The connector speaks pgoutput protocol version 1 in text mode (binary tuple values are refused). Each tuple carries one cell per column *of the relation*, so a width mismatch means cached relation metadata is stale and is refused rather than mis-decoded (`src/decode.rs:337`).

A pre-image question: for UPDATE and DELETE the server emits **at most one** of an old tuple or a key tuple, chosen by the table's replica identity:

- **FULL** → `PreviousRow::Full`: every column of the old row.
- **DEFAULT / identity index** → `PreviousRow::Key`: the key tuple. Its cells for non-identity columns are NULLs that *must not* be forwarded as real NULLs, so `decode_key_row` keeps only the identity columns (`src/decode.rs:377`).
- **nothing logged** → `PreviousRow::Absent`: the update did not rewrite the key.

### TOAST holes — PostgreSQL's most surprising UPDATE

When an UPDATE does not rewrite a TOASTed column (a large text/json/bytea value that was moved out of line), pgoutput sends `UnchangedToast` for that cell instead of a value. Sending a placeholder (or the empty string, or dropping the column) would silently corrupt the row. So `fill_toast_holes` (see Invariant 5) requires a **FULL** old image to fill each hole from; against a thinner identity the update is refused with the repair hint `REPLICA IDENTITY FULL` (`src/decode.rs:425`).

### Absent pre-image and the silent-double-count path

For an UPDATE with no pre-image against a **keyed** Feldera table the old row is replaced by the insert-by-key, which *is* exactly the old+new pair — so a single insert is sent. Against a **keyless** Feldera table the old row would keep sitting next to the new one and double-count silently; that path is refused (this is the fix from the architecture review, `src/feldera.rs:64`).

### Type coverage

Decoded into one `Value` enum fed by the same path for COPY (snapshot) and pgoutput (stream): `bool`, `int2/4/8`, `float4/8`, `numeric` (kept as a string to preserve precision; `NaN`/`Infinity` → null, since JSON has none and Feldera maps them to null), `text`, `varchar`, `bpchar`, `name`, `date`, `time`, `timestamp`, `timestamptz`, `uuid`, `json`/`jsonb`, `bytea` (hex, emitted as an array of bytes to match Feldera's default JSON flavor). Anything else — a type OID not in the built-in table — is refused (`src/decode.rs:87`).

The `timestamptz` handling has a subtle reason: the snapshot reads with one PostgreSQL session and the walsender streams with another, and their `TimeZone` settings can differ. All `timestamptz` values are therefore shifted to UTC in the decoder so both paths produce identical bytes (`src/decode.rs:197`).

### COPY decoding

`COPY ... (columns) TO STDOUT` output arrives in arbitrary chunks, so a partial row at a chunk boundary is kept until the rest turns up; `COPY` terminates every row with a newline, so anything left at the end is an unterminated row and an error, not silence (`src/connector.rs:464`). The explicit column list makes COPY order equal to the catalog query order, and COPY's `\N` null marker plus escape rules are decoded with the *same* value decoder as the replication stream, so a snapshot row and a streamed row agree (`src/decode.rs:579`).

## Layer 3 — Contract and routing

### The Feldera contract is discovered, not assumed

At startup the connector fetches the compiled pipeline and reads each destination's declared primary key and fields from `program_info.schema.inputs` (`src/feldera.rs:317`). Identifiers are compared with awareness that Feldera folds unquoted names to **upper** case while PostgreSQL folds to **lower** case — comparisons are case-insensitive unless the identifier declares itself case-sensitive (`src/contract.rs:60`).

### Replica identity must cover Feldera's key

If the Feldera table is keyed by `(REGION, ID)` but PostgreSQL's replica identity is only `id`, a delete's pre-image cannot name the full key. This is checked at startup (`snapshot_column_metadata` reads `is_key` from the catalog) and again on every `RELATION` message (`src/contract.rs:10`), because DDL can change the identity while the connector runs. When identity does not cover the key, the connector refuses rather than send a delete Feldera would silently ignore.

### Routing by relation ID

Each `RELATION` message carries a relation ID; the decoder keeps a map ID → (schema, table, columns), and every change routes through it to a single configured ingress. A published table that is *not* in `TABLES` makes `route_index_for` fail the session at its first `RELATION` — deliberately: every row from an unknown table would otherwise be filed under the wrong name (`src/contract.rs:233`). Symmetrically, startup checks that every `TABLES` entry actually appears in the publication, because an unlisted table would be snapshotted once and then never updated (`src/snapshot.rs:212`).

## Layer 4 — The bounded outbox

PostgreSQL transactions have no size limit, so buffering a whole transaction is an unbounded-memory bug waiting for the first 100-million-row transaction. The outbox is deliberately bounded and does not grow (`src/outbox.rs`). A batch flushes when:

- it reaches `BATCH_MAX_ROWS`,
- the oldest event reaches `BATCH_LINGER_MS` (`0` sends per transaction), or
- a transaction COMMIT arrives and the batch is due.

Events are grouped per Feldera table, preserving first-appearance order, and each route becomes one HTTP request.

**Oversized transactions** (bigger than the bound) are flushed mid-transaction in pieces (`src/connector.rs:1216`). The commit boundary is lost, but the alternative is a permanently stuck connector. The position is deliberately left put, so a crash replays the *whole* transaction and the destination's primary key absorbs the portion that arrived twice — atomicity is weakened without sacrificing the no-loss guarantee.

TRUNCATE, for its part, discards the outbox: the source already discarded those rows, so replaying them forward would resurrect deleted data. The outbox is cleared rather than flushed (see below).

## Layer 5 — The Feldera sink

- **Ingress**: one request per route, NDJSON of `{"insert": row}` / `{"delete": row}` events (an UPDATE is *two* events: delete of the old row plus insert of the new, so it stays correct whether or not the identity changed — `src/feldera.rs:24`).
- **Durability**: a successful POST is *acceptance only*. The response body's completion token is polled at `/completion_status` with an exponential cap until it reports `complete`. A missing token is a hard failure, never a fallback to treating acceptance as safety (`src/feldera.rs:425`).
- **Error classification**: network errors and 5xx/timeouts/429 are `Unavailable` (retry with backoff); 4xx are `Rejected` (the bytes are wrong, retrying the identical bytes is pointless — `src/feldera.rs:222`).
- **Slow sink**: once a wait crosses `SLOW_WAIT_WARNING` (5s, `src/feldera.rs:179`) the connector prints that WAL is accumulating on the source while it blocks. This wait *is* the backpressure: the connector simply stops reading the stream, and PostgreSQL's replication connection pushes back from there.
- **Warnings**: during the wait the keepalive still flows, always announcing the truthfully old flush position.

## Layer 6 — Snapshot and recovery

### One consistent point

The snapshot is not a COPY that happens to be followed by a stream. Order matters:

1. create the slot with an exported snapshot,
2. open a `REPEATABLE READ` transaction on a second connection and `SET TRANSACTION SNAPSHOT '...'` to it,
3. `COPY` the table inside that transaction,
4. commit the reader,
5. only then release the replication connection that owns the snapshot.

The stream later resumes at the slot's exact `consistent_point` — the snapshot and the first WAL delivered describe the *same instant*.

### Snapshot-as-replacement

A replication slot tracks a WAL position but says nothing about whether a previous snapshot completed. So the connector keeps a **durable state file** (`SNAPSHOT_STATE_PATH`), written atomically (temp file, fsync, rename, fsync the directory — `src/snapshot.rs:130`) *before* any row moves. Startup decision-making is a truth table over exactly two facts — the state file and slot existence (`plan_startup`, `src/snapshot.rs:345`):

| state file says | slot exists | decision |
| --- | --- | --- |
| complete | yes | **Resume** the stream — the target provably holds everything up to the slot. |
| complete | no | The slot was the only thing retaining WAL since the last ack; it is gone, so that window is **unrecoverable** — warn loudly and re-snapshot from a new slot. |
| pending | yes | A snapshot died mid-flight; take a fresh one from a new consistent point. |
| nothing / no record | yes | A slot proves a position but not that the target was seeded — drop it and re-snapshot (or set `SNAPSHOT_MODE=never` deliberately). |

A snapshot is a **replacement, not a delta**: before COPY, the connector retracts every row Feldera currently holds. The row set to retract is Feldera's *live* query of the destination, unioned with the keys this process already made durable (a degenerate partial snapshot must not leave duplicates — `src/connector.rs:494`). This is what keeps a failed snapshot from leaving stale rows standing next to the fresh COPY's rows.

`SNAPSHOT_MODE=inconsistent` exists only as a demonstration: it reads the table before creating the slot, so every commit in the window between the read and the slot's consistent point is lost *silently and permanently*. The connector states this in a warning every time it is run (`src/connector.rs:290`).

## Layer 7 — Supervision (main.rs)

Outside the session is a small supervisor:

- **Startup gating**: `prepare` runs the state machine above, verifies the publication exists and covers every `TABLES` entry (and warns when it advertises `truncate` off, so TRUNCATE would silently strand rows in Feldera — `src/snapshot.rs:177`), and validates each destination contract.
- **Reconnect**: sessions end on disconnect and restart with jittered exponential backoff (200ms→30s). Only real progress (the flush position advancing) resets the backoff; a connector that connects and drops repeatedly without progress gives up after `RECONNECT_GIVE_UP_SECS` and exits nonzero rather than holding the slot open indefinitely (`src/main.rs:206`).
- **Graceful shutdown**: SIGINT/SIGTERM stop the loop. `finish` sends a final standby status update, then **re-reads the slot over a second connection** — a status update has no reply in the protocol, so reading `confirmed_flush_lsn` back is the only confirmation on offer; if the slot has not caught up within `SHUTDOWN_DEADLINE` the connector exits anyway and accepts redelivery, with the key as the absorber (`src/connector.rs:1277`).
- **Fault injection**: `CRASH_AT` kills the process at one of five points (`during-snapshot`, `before-send`, `after-send`, `after-durable`, `after-ack`) to prove every recovery path — the harness runs a full matrix of these (`scripts/harness.py`).

## TRUNCATE is a retraction, not a delete

TRUNCATE carries relation IDs, no rows, and discards those rows' WAL entirely. Three things happen (`src/connector.rs:1139`):

1. any unsent buffered events for the affected routes are **dropped** — replaying them would resurrect rows the source already erased;
2. Feldera's live rows are queried and retracted (the same mechanism a replacement snapshot uses), chunked by `BATCH_MAX_ROWS`;
3. where a TRUNCATE names relations that never appeared in a cached `RELATION`, the connector retracts *every* configured route rather than guess (`src/contract.rs:126`).

## Stated invariants

The `docs/invariants.md` file pins the contract in six properties, each enforced at the code sites named there:

1. **Durable-before-ack** — the slot's flush LSN only advances after Feldera's completion token; graceful shutdown re-reads the slot to confirm.
2. **Monotonic position** — `received`/`flush` never decrease and never run ahead of received-but-not-durable changes.
3. **Snapshot is a replacement**, gated by the startup state machine.
4. **Keyed deletes** — every delete (and update-old) names the full Feldera primary key, or it is refused.
5. **No TOAST holes** — an `Unchanged` cell is filled from a FULL old image or the change is refused; it never serializes.
6. **Bounded memory and honest backpressure** — buffering is bounded on every path; Feldera slowness becomes waits plus truthful keepalives, never an unbounded queue.

## Known limitations (accepted, documented in `docs/architecture-review.md`)

- **Retraction scales O(table)**: a snapshot of a pre-populated table, or a TRUNCATE, queries all of Feldera's live rows into memory at once. Correct, but paginated/streamed retraction is the first thing to reach for if a table outgrows the arrangement.
- **No cross-table atomicity**: a batch spanning two routes is two HTTP requests; a crash between them is healed by replay plus primary-key idempotency.
- **Reconnect drops buffered events by design** — they were never acknowledged, so they replay from the slot.

## Benchmarks

`docs/benchmarks.md` measures the decode/encode pipeline in-process and the live snapshot/stream/txn paths end to end through a real Feldera (results from the README's own run):

| axis | result |
| --- | --- |
| decode COPY row | 671k rows/s |
| row → JSON | 235k rows/s |
| encode change events | 1.65M events/s |
| initial snapshot → Feldera | 62k rows/s |
| streaming (small txns) | 8.3k events/s |
| one giant transaction | 9.8k events/s (bound by Feldera's completion round-trips) |

The sharp edge is startup reconcile: a large leftover row set in Feldera becomes one long serial delete-batch wake before the snapshot, so `SNAPSHOT_BATCH_ROWS` should stay small enough that a delete batch fits inside Feldera's ingest window.

## Configuration

### Tables

```text
TABLES=users                              # one table
TABLES=users,items                        # several
TABLES=users:USERS,public.items:LINEITEMS # explicit PG:Feldera mapping
```

`PG_TABLE` / `FELDERA_TABLE` / `PG_SCHEMA` configure a single table instead of `TABLES`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `TABLES` | — | Comma separated `source[:DESTINATION]` pairs. |
| `PG_TABLE` | value of `FELDERA_TABLE` | Source table for single-table mode. |
| `FELDERA_TABLE` | `users` | Destination table for single-table mode. |
| `PG_SCHEMA` | `public` | Schema for unqualified table names. |

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
    connector.rs  the replication session: stream loop, flush, durability wait, retraction, finish
    contract.rs   Feldera ↔ PostgreSQL contract: routing, keyed-delete enforcement, identity checks
    decode.rs     pgoutput + COPY decoding, TOAST-hole filling, type coercion, JSON conversion
    feldera.rs    HTTP sink: ingress, completion tokens, live-row queries, error classification
    outbox.rs     bounded per-route buffer, size/linger/commit flush decisions
    runtime.rs    positions, standby status, reconnect bounds, slot confirmation, crash points
    snapshot.rs   slot creation, exported snapshot, state file, startup truth table, COPY
docs/
    invariants.md           the six-property contract, with code locations
    architecture-review.md  what was audited and the one fix it produced
    benchmarks.md           decode/encode plus live end-to-end numbers
scripts/
    harness.py              live harness: fault matrix, fuzzer, benches, smoke
```

## Running

Prerequisites on PostgreSQL:

```sql
CREATE PUBLICATION my_pub FOR TABLE users WITH (publish = 'insert, update, delete, truncate');
ALTER TABLE users REPLICA IDENTITY FULL;  -- or a primary key covering Feldera's key
```

Slots are created by the connector. The publication must carry `truncate` or TRUNCATE will silently strand rows in Feldera.

Prerequisites on Feldera: pipeline compiled and running, destination table materialized with a PRIMARY KEY.

```sh
export POSTGRES_URL="host=localhost user=postgres password=postgres"
export TABLES="users"
export FELDERA_URL="http://localhost:8080"
export FELDERA_PIPELINE="JVM"
cargo run
```

Build and test:

```sh
cargo build
cargo test
```

The live harness (`scripts/harness.py`) runs the crash-point matrix, a randomized fuzzer, a smoke test, and the three benchmarks — see `docs/benchmarks.md`.