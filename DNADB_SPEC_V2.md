# DNADB Spec V2 (Implemented Architecture)

This document supersedes projection-only notes with the architecture that is now implemented and benchmark-validated.

## Raw Journal + Materializer Architecture

DNADB uses a two-phase write path for high-throughput ingest:

1. **Fast append (ingest path)**
   - Raw JSON docs are appended to `raw_journal/<collection>/raw.wal` via `LsmWritePipeline`.
   - This path avoids MVCC/index maintenance on the request critical path.
2. **Background apply (materializer path)**
   - A materializer replays WAL-order records into the durable MVCC store.
   - Materialization preserves deterministic sequence ordering and updates query-visible state.

The runtime tracks per-collection materialization checkpoints in `materialize_state.json`:
- `last_applied_wal_sequence`
- last non-empty apply wall-time and record-count stats

This yields bounded write latency while preserving ordered durability and eventual query visibility.

## Direct Apply Path

Materialization now uses a direct apply fast path:

- `EngineRuntime::apply_materialized_docs_direct(...)`
- `DurableTransactionStore::execute_insert_many_materialize_direct(...)`

Key behavior:
- Skips second append/write-pipeline loops during materialization.
- Applies already decoded records directly to MVCC and incremental indexes.
- Uses optimized existence checks and MVCC insert behavior for monotonic write timestamps.

Result: materializer work is dominated by pure apply/index cost, not duplicate write plumbing.

## Bounded Batch Scheduler

Production scheduler behavior is bounded and lock-aware:

- **Batch size:** `512`
- **Max batches per tick:** `4`
- **Tick interval:** `10ms`

Execution shape per tick:
1. Decode each bounded batch from raw WAL.
2. Hold runtime lock only for direct apply of decoded docs.
3. Release lock between per-batch rounds.
4. Perform storage sync in a separate lock scope after apply rounds.

This design keeps lock hold times short and prevents long critical sections from decode/fsync work.

## Validated Performance Characteristics

The following values are measured outputs from the implemented harness and runtime path.

| Metric | Start | Current | Improvement |
|---|---:|---:|---:|
| Writes (strict) | 54/s | 136,000/s | 2,518x |
| Write scaling | O(N^2) | O(N) | fixed |
| P99 ingest latency | 3,353ms | 5.55ms | 604x |
| Lock hold P99 | 3,282ms | 5.68ms | 578x |
| CMS seed @ 200k | 204s | ~22s | 9x |

1M scenario E validation:
- `rows_ingested_end = 1,000,000`
- `lag_p99_ingest = 0.0`
- `end_to_end_client_ms_p99_ingest = 5.551333`
- `rt_lock_hold_ms_p99_ingest = 5.676875`
- No measurable drain backlog (`lag_max = 0`)

Operational conclusion: ingest/materializer behavior is linear at the validated 1M tier with zero ingest-phase lag accumulation.

## Group Commit Tuning

Group-commit semantics are explicitly documented and currently tuned as:

- Raw ingest: append-first with deferred materialization apply.
- Durable apply: bounded incremental commits with deferred sync orchestration.
- Deferred sync is explicitly flushed via runtime sync calls and rebuild boundaries.

Current validated tuning:
- Keep apply batches small and frequent (`512 x 4 @ 10ms`) to reduce lock hold variance.
- Separate sync from apply critical sections.
- Use relative + rate-aware recovery criteria in benchmark analysis for stable convergence detection.

## Production Defaults

Promoted defaults:

```toml
[materializer]
batch_size = 512
max_batches_per_tick = 4
interval_ms = 10
```

These values are no longer experimental benchmark knobs; they are the validated baseline for current production behavior.
