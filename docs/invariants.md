# Invariants

The connector's contract in six properties. Each is enforced by the listed
code; later phases encode them as tests that fail when the enforcement is
removed.

## 1. Durable-before-ack (at-least-once)

PostgreSQL is only told a batch is safe after Feldera confirms it fully
processed (`FELDERA_ACK_MODE=completed`). Acceptance alone is never treated as
safety, except under the explicitly loss-prone `accepted` mode.

- `src/feldera.rs`: `AckMode` (149), `Barrier` (159), `send()` (394) returns a
  token or rejects (420), `wait()` (495) polls the completion token to
  `complete`.
- `src/connector.rs`: `flush()` (688) sends (721), waits for durability
  (733-735), and only then advances `position.flush` and acks (766-768).
- `src/connector.rs`: `wait_for_durability` (630) reports the *old* flush
  position while waiting (645-654) — a keepalive that lies is worse than none.
- `src/main.rs`: warns that `accepted` mode can lose data (47-55).

## 2. Monotonic position

`position.flush` never decreases, and never runs ahead of changes that are
received but not yet durable.

- `src/connector.rs` (759-764): `committed_through <= flush` moves nothing.
- `src/connector.rs` (1117-1119): the COMMIT boundary is recorded
  unconditionally; only a flush with an empty outbox acts on it.
- `src/connector.rs` (1245-1254): a keepalive `wal_end` is claimed only with no
  transaction open and nothing buffered.
- `src/connector.rs` (1211-1236): an oversized transaction flushes early but
  leaves the position put, so a crash replays the whole transaction and the
  destination key absorbs the re-sent part.

## 3. Snapshot is a replacement, gated by the startup state machine

Resuming happens only when the slot *and* a complete state file together prove
the target holds everything up to the slot position. Every seeding path
retracts Feldera's live rows first, then replaces them.

- `src/snapshot.rs`: `plan_startup` (347-421) is the truth table; the pending
  case is "a snapshot is a replacement, not a delta" (408-409).
- `src/snapshot.rs`: `write_snapshot_state` (130-159) — temp file, fsync,
  rename, fsync the directory.
- `src/connector.rs`: state written "pending" before any row moves (223);
  `copy_table_into_sink` (358-379) retracts each target (521-564) before COPY.
- `src/contract.rs`: retraction = live query ∪ durable session keys
  (191-229), so a partial earlier snapshot cannot leave duplicates.

## 4. Keyed deletes

Every delete (and update-old) Feldera receives names the full Feldera primary
key. An unkeyed delete is refused, never sent.

- `src/contract.rs`: `check_delete_carries_key` (307-337) refuses.
- `src/feldera.rs`: `feldera_change_events` (24-68) calls it before any
  delete/update-old event.
- `src/contract.rs`: `refuse_unusable_identity` (10-34), `report_identity_coverage`
  (261-303) at startup.
- `src/main.rs`: a keyless Feldera table is refused by default (68-95).

## 5. No TOAST holes

An `Unchanged` cell never serializes to the wire: it is filled from a FULL old
image, or the change is refused.

- `src/decode.rs`: `fill_toast_holes` (425-483) errors without a full old image
  (442-459) and refuses old images that are holes too (471-477).
- `src/decode.rs`: `row_to_json` (489) errors on `Value::Unchanged` (517-522).
- `src/decode.rs`: `decode_value` (313) maps `UnchangedToast` to
  `Value::Unchanged` (323-325).

## 6. Bounded memory and honest backpressure

Buffering is bounded by `BATCH_MAX_ROWS` and `SNAPSHOT_BATCH_ROWS` on every
path; Feldera slowness becomes backpressure (wait + truthful keepalives), never
an unbounded queue.

- `src/outbox.rs`: `due()`/`grouped()` (61-87); `flush()` clears the outbox
  (`src/connector.rs` 756).
- `src/main.rs`: `batch_max_rows` clamped >= 1 (135-137).
- `src/connector.rs`: mid-transaction early flush (1217-1236); snapshot batches
  (574-627) bounded by `snapshot_batch_rows`; TRUNCATE drops the events the
  source already discarded (1159-1169).
- `src/feldera.rs`: completion poll is time-bounded (541-548); `SLOW_WAIT_WARNING`
  (171).