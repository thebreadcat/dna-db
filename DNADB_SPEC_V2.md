# DNADB Spec V2 (Implemented Architecture)

This document describes the **single durable write path** in the engine: all inserts go through `DurableTransactionStore` (collection WAL, strand materialization, MVCC, indexes). There is **no separate raw journal** or background materializer.

## Write path

1. Client sends documents (HTTP bulk, wire insert, etc.).
2. `commit_inner` (or `execute_insert_many` / `execute_mongo_insert_many_with_mode`):
   - Append each operation to the collection **durable WAL** (`<collection>.wal`).
   - Materialize to strand/complement/meta files (`process_wal_entry_with_mode`).
   - `flush_maps`, optional deferred `sync_files`, then `wal.sync()` per commit policy.
   - `tx_manager.commit` and incremental index updates (`apply_sort_index_writes`).

Success returned to the client implies the configured durability mode has been met (strict vs deferred strand fsync).

## Read path

Queries compile to `QueryAst`, use MVCC visibility, sort/exact/trigram/composite indexes as configured, then overlay masking on the HTTP wire.

## Startup

1. Open `DurableTransactionStore`: scan strand pool, open WAL.
2. `replay_wal_to_mvcc`: rebuild in-memory MVCC from durable WAL entries.
3. `rebuild_sort_indexes` (and related index structures) from visible MVCC state.
4. HTTP server may **pre-warm** sort index probes before accepting traffic (`dnadb_http --prewarm-indexes`).

## Compaction (future / checklist #8)

Raw-segment merge and `raw_journal/` compaction are **removed**. WAL rotation / sealing old WAL files after replay is the intended simplification; not all of that may be implemented yet—see `RELEASE_CANDIDATE_CHECKLIST.md`.

## Historical note

Earlier versions used a two-phase **raw journal + materializer** design for ingest. That path duplicated IO (raw + durable) after crash-safety required full durable persistence on apply. The project **dropped the raw journal** in favor of direct durable writes for simpler correctness and better throughput on the validated CMS seed path.
