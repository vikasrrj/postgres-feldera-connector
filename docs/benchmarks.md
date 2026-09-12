# Benchmarks

Measured on the harness stack: `harness-pg` (PostgreSQL 18, logical WAL, slot per run),
`feldera` (pipeline-manager, 1 worker), and this connector built `--release`
(`./target/release/postgres-cdc`). The harness lives in `scripts/harness.py`
(`bench-snapshot`, `bench-stream`, `bench-txn`, plus an in-process `#[ignore]`
micro-benchmark in `src/decode.rs`).

| axis | workload | result |
| --- | --- | --- |
| decode COPY row | 200k rows in-process | 671k rows/s |
| row -> JSON | 200k rows in-process | 235k rows/s |
| encode change events | 800k events in-process | 1.65M events/s |
| initial snapshot | 50k rows -> Feldera | 62k rows/s (0.80s) |
| streaming (small txns) | 40k events post-snapshot | 8.3k events/s (4.84s) |
| one giant transaction | 100k-row single INSERT -> Feldera | 9.8k events/s (10.18s) |

Method: every run seeds PostgreSQL first (snapshot) or seeds a slot on a quiet
PostgreSQL (stream/txn), then the connector warms to "Streaming from slot" and
the timer starts. The stream and txn runs finish when Feldera's materialized
`stock` equals PostgreSQL's rows (two stable polls), so the numbers include
Feldera's ingestion + completion latency end to end. Release build, default
batching (`BATCH_MAX_ROWS=10000-25000`, 1s flush interval).

## Findings

- End-to-end throughput (8-10k events/s) is bounded by Feldera's per-batch
  ingestion + completion round-trips, not by decoding or encoding (both >200k
  rows/s per core). Snapshot ingestion is the fast path (~62k rows/s) because
  COPY batching is much larger than the streaming flush interval historically.
- Startup reconcile is the sharp edge: when Feldera already holds rows this
  connector does not, it retracts them in DELETE batches before streaming.
  A batch larger than Feldera's ~30s ingest window returns 503 and is retried
  with backoff (`Snapshot batch N failed (...); retrying`). On a healthy
  pipeline the reconcile converges, but it is serial before the snapshot, so a
  large leftover set extends time-to-converge by that whole interval. Keep
  `SNAPSHOT_BATCH_ROWS` small enough that a delete batch stays inside Feldera's
  window when the destination already has many rows.
- Harness hygiene matters: consecutive benches poison the next one's reconcile
  unless the Feldera pipeline is restarted (community edition blocks
  `stop`/`delete`, so a clean slate means restarting the container).