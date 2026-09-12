# Architecture review

Reviewed after Phases 1-4 were green (invariants defined, 53 unit tests, all
five crash points, live fuzzing). Verdict: the pipeline is sound; every
ack-visible guarantee is either enforced or refused. No rewrite warranted.

## What was checked

- **Durable-before-ack** (`connector.rs` flush/wait_for_durability/finish):
  the slot's flush LSN only advances after Feldera's completion token, and a
  graceful shutdown re-reads the slot to make sure the last update landed.
- **Monotonic position**: `received`/`flush` are max'd everywhere; keepalive
  advances only when the outbox is empty and no transaction is open, so the
  slot never runs ahead of sent-but-unsafe changes.
- **Snapshot replacement rule** (`snapshot.rs` plan_startup): every non-Resume
  decision is derived from exactly two facts (state file + slot existence) and
  either reseeds pending work or warns loudly about unrecoverable WAL.
- **Keyed deletes** (`contract.rs` check_delete_carries_key): an unkeyed
  delete would be silently ignored by Feldera, so it is refused.
- **Toast holes** (`decode.rs` fill_toast_holes): a hole is filled from a FULL
  old image or the run fails; there is no silent placeholder.
- **Bounded batching** (`outbox.rs`): the outbox flushes on size, linger, or
  commit; oversized transactions flush mid-transaction rather than grow the
  buffer without limit.
- **Routing/retraction** (`contract.rs`): retract-all reads Feldera's live set
  direct and merges the session's durable keys as a degenerate-case fallback.

## One fix made

`feldera.rs` `CdcEvent::Update` when PostgreSQL logs no pre-image (the common
case: DEFAULT identity and the update does not rewrite the key):

- Feldera table **keyed** -> the old row is implicitly replaced by the insert
  (primary-key aggregation), which equals the old + new pair. Keep inserting.
- Feldera table **keyless** -> the old row would survive next to the new one
  and silently double-count. Now refused with a repair hint. This closes a
  silent-divergence path that the startup warning could not catch.

Covered by `update_with_no_previous_row_is_accepted_when_keyed` and
`update_with_no_previous_row_is_refused_when_keyless`.

## Known limitations (accepted, documented)

- Retraction (snapshot of a pre-populated table, or a TRUNCATE) queries all of
  Feldera's live rows into memory at once. Correct, but O(table) on both ends;
  a very large table maps to a very large retract request. Paginated/streamed
  retraction would bound this and is the first thing to reach for if a table
  outgrows the arrangement.
- No cross-table atomicity at Feldera: a batch spanning two routes is two HTTP
  requests. A crash between them is healed by replay + primary-key idempotency.
- Disconnect/reconnect: the session rebuilds relations and drops buffered
  events (never acknowledged), which replay from the slot position. By design.