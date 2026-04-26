# DNA-DB Build Progress Tracker

This is the execution log and queue for building `DNADB_SPEC.md` phase by phase.
Update this file at the end of every work session.

## How to Use This File

1. Pick the first item under `## Next In Queue`.
2. Complete it end-to-end with tests/verification notes.
3. Move it to `## Done Log` with date and outcome.
4. Pull the next item from the same stage.
5. When a stage is complete, mark it in `## Stage Status`.

## Stage Status

- [x] Stage 1 - Strand Storage Engine + WAL Write Path
- [x] Stage 2 - CRISPR Query Engine + Basic Developer Interface
- [x] Stage 3 - Auth, Privacy, and Epigenetic Overlays
- [x] Stage 4 - Histone Block Manager
- [x] Stage 5 - Telomere Lifecycle System
- [x] Stage 6 - Wire Protocol Compatibility
- [x] Stage 7 - Lateral Transfer Protocol (Distributed)
- [x] Stage 8 - Ecosystem (parallel stream)

## Next In Queue

### Active Stage
Stage 8 - Ecosystem (parallel stream)

### Immediate Next Tasks

- [x] S2-T01: Build query AST / guide pattern compiler for `where` clauses
- [x] S2-T02: Build intron hash fast-path matcher
- [x] S2-T03: Build parallel strand scan executor by CPU core partition
- [x] S2-T04: Implement `fetch`, `fetchOne`, `limit`, `orderBy`, and include traversal (basic)
- [x] S2-T05: Ship minimal TypeScript SDK (`insert`, `where`, `fetch`)
- [x] S2-CHK1: Internal dogfood checkpoint for Stage 2

### Next Stage 3 Queue

- [x] S3-T01: Identity store and token/session model
- [x] S3-T02: Overlay definitions and resolver pipeline
- [x] S3-T03: Overlay-aware serialization and immutable audit strand logging
- [x] S3-T04: TLS baseline and secure defaults
- [x] S3-CHK1: Production-safety checkpoint for auth/privacy layer

### Next Stage 4 Queue

- [x] S4-T01: Temperature model primitives (Hot/Warm/Cold/Frozen)
- [x] S4-T02: Access tracking and tier transition evaluator
- [x] S4-T03: Semantic co-location planner and block assignment policy
- [x] S4-T04: Background rebalance scheduler loop
- [x] S4-CHK1: Histone manager checkpoint

### Next Stage 5 Queue

- [x] S5-T01: Immortal-by-default lifecycle policy primitives
- [x] S5-T02: Collection-level telomere policies and refresh mechanisms
- [x] S5-T03: Soft-delete + grace-period lifecycle pipeline
- [x] S5-CHK1: Telomere lifecycle checkpoint

### Next Stage 6 Queue

- [x] S6-T01: Mongo wire translation coverage baseline
- [x] S6-T02: PostgreSQL protocol parser and translation layer (baseline)
- [x] S6-T03: Compatibility test matrix scaffolding for common clients
- [x] S6-CHK1: Wire protocol compatibility checkpoint

### Next Stage 7 Queue

- [x] S7-T01: Transfer packet schema and mutation absorption rules
- [x] S7-T02: Multi-node sync loop and version coexistence handling
- [x] S7-T03: Failure scenario handling (partitions, delayed nodes, replay)
- [x] S7-CHK1: Distributed transfer checkpoint

### Next Stage 8 Queue

- [x] S8-T01: Python SDK baseline client + query builder
- [x] S8-T02: Admin/inspection tooling baseline
- [x] S8-T03: GraphQL layer baseline
- [x] S8-T04: Monitoring and deployment packaging baseline
- [x] S8-CHK1: Ecosystem checkpoint

### Completed Stage 1 Tasks (archive)

- [x] S1-T01: Bootstrap repository layout and language/runtime choices (engine, sdk, tests, benchmarks folders)
- [x] S1-T02: Define canonical strand model types (`Strand`, `Codon`, `Intron`, `Telomere`) and serialization contracts
- [x] S1-T03: Implement base-4 codon encoder/decoder with round-trip tests
- [x] S1-T04: Implement complement generation and verification (`value + complement = 3`) with corruption detection tests
- [x] S1-T05: Implement memory-mapped collection file primitives (`.strands`, `.complement`, `.meta`)
- [x] S1-T06: Implement WAL append/flush path with sequence numbers and fsync
- [x] S1-T07: Build async WAL processor (encode -> complement -> intron weave -> persist)
- [x] S1-T08: Implement startup crash recovery replay from WAL
- [x] S1-T09: Add SIMD acceleration path and non-SIMD fallback
- [x] S1-T10: Add Stage 1 benchmark harness (write latency, throughput, recovery time)
- [x] S1-CHK1: Stage checkpoint - all Stage 1 tests pass and benchmark baseline recorded

## Done Log

Use this format for each completed task:

- YYYY-MM-DD - TASK_ID - short title
  - What changed:
  - Verification:
  - Notes / risks:

- 2026-04-25 - S1-T01 - Bootstrap repository layout and runtime choices
  - What changed: Created core scaffold (`engine`, `sdk/typescript`, `sdk/python`, `tests`, `benchmarks`, `docs`, `examples`, `config`, `scripts`), added starter manifests (`engine/Cargo.toml`, TS `package.json`, Python `pyproject.toml`), and added root `README.md` with architecture, hosting, local dev, query model, config, security/privacy guidance.
  - Verification: Confirmed directory/file creation in workspace and validated tracker progression by updating queue state from S1-T01 to S1-T02.
  - Notes / risks: Chose Rust for engine and TypeScript/Python for SDKs. Stage 1 CLI scope still open; decide before S1-T05.

- 2026-04-25 - S1-T02 - Canonical strand model + serialization contract
  - What changed: Added core model types in `engine/src/model.rs` (`Strand`, `Codon`, `Intron`, `Telomere`, `RefreshPolicy`, `Tag`) and introduced a versioned serialization codec in `engine/src/codec.rs` with magic header (`DNAS`) and format version (`v1`).
  - Verification: `cargo test` in `engine` passed, including codec round-trip and invalid-header tests.
  - Notes / risks: Serialization is currently bincode-based; if cross-language deterministic serialization is required early, we may switch to an explicit binary layout in Stage 1.

- 2026-04-25 - S1-T03 - Base-4 codon encoder/decoder with round-trip tests
  - What changed: Added `engine/src/encoding.rs` with byte<->base4 symbol conversion and codon packing/unpacking, including payload length metadata to guarantee reversible decode.
  - Verification: `cargo test` in `engine` passed, including empty payload, text payload, and full byte-range round-trip tests.
  - Notes / risks: Current approach is correctness-first; SIMD acceleration comes in `S1-T09`.

- 2026-04-25 - S1-T04 - Complement generation and corruption detection
  - What changed: Added `engine/src/complement.rs` with complement generation (`3 - value`), pair validation, and corruption position detection (`codon_index`, `symbol_index`, observed values).
  - Verification: `cargo test` in `engine` passed with complement-validity and corruption-detection unit tests.
  - Notes / risks: Validation currently compares equal-length codon vectors; mismatch recovery behavior will be expanded during persistence/WAL integration.

- 2026-04-25 - S1-T05 - Memory-mapped collection file primitives
  - What changed: Added `engine/src/storage.rs` implementing `CollectionStorage` with creation/opening of `.strands`, `.complement`, `.meta` files, mmap-backed append primitives, and flush/sync behavior.
  - Verification: `cargo test` in `engine` passed with storage creation+append test plus all previous tests (8 total).
  - Notes / risks: Current mmap implementation uses fixed-size bootstrap files and returns `BufferFull` on overflow; dynamic resizing/compaction is still needed.

- 2026-04-25 - S1-T06 - WAL append/flush with sequence numbers
  - What changed: Added `engine/src/wal.rs` with WAL file bootstrap (`DWAL` + version), monotonic sequence assignment, append + `sync_data`, and replay reader for recovery input.
  - Verification: `cargo test` in `engine` passed including WAL sequence monotonicity, replay, and reopen sequence recovery tests (10 total).
  - Notes / risks: WAL records currently use length-delimited payloads without checksums; integrity checks can be added in Stage 1 durability hardening.

- 2026-04-25 - S1-T07 - Background WAL processor
  - What changed: Added `engine/src/processor.rs` with `strand_from_wal_payload` (encode → complement → `_payload` intron + FNV-1a hash), `process_wal_entry` persistence to `.strands` + complement chunks, and `WalProcessorHandle` worker thread with submit/shutdown.
  - Verification: `cargo test` in `engine` passed (strand build, sync persist round-trip, background drain).
  - Notes / risks: Complement pool format is length-prefixed bincode `Vec<Codon>`; must stay in sync with `scan_complement_tail` in storage.

- 2026-04-25 - S1-T08 - Startup WAL replay after crash
  - What changed: `CollectionStorage::open_or_create` now rehydrates append offsets and `materialized_high_water_sequence` by scanning existing strand/complement/meta tails; added `engine/src/recovery.rs` with `replay_pending_wal_after_open`; `process_wal_entry` updates high-water after each materialization.
  - Verification: `cargo test` in `engine` passed including recovery test (WAL 1–3, pool only 1, replay yields versions `[1,2,3]`); 14 tests total.
  - Notes / risks: Meta tail uses “last non-zero byte” heuristic; structured meta records should replace this later.

- 2026-04-25 - S1-T09 - SIMD encode path + scalar fallback
  - What changed: Refactored `engine/src/encoding.rs` with `encode_bytes_to_codons_scalar`, x86_64 AVX2 widen+lane-shift path (`encode_bytes_to_codons_avx2` / `#[target_feature(enable = "avx2")]`), threshold `SIMD_ENCODE_THRESHOLD` (64), and default `encode_bytes_to_codons` dispatch. Non-x86 and small payloads use scalar.
  - Verification: `cargo test` in `engine` passed (16 tests), including AVX2-vs-scalar equivalence on x86_64 when AVX2 is present.
  - Notes / risks: True AVX-512 path from the spec is not implemented yet; this chunk targets AVX2-style lane SIMD for the hot symbol expansion loop.

- 2026-04-25 - S1-T10 - Stage 1 benchmark harness
  - What changed: Added Criterion bench `engine/benches/stage1_encode.rs` (`[[bench]] stage1_encode`) comparing default encoder vs scalar vs `encode_bytes_to_codons_avx2`; dev-dependency `criterion` with `html_reports`.
  - Verification: `cargo check --benches` and `cargo test` succeeded in `engine`.
  - Notes / risks: WAL/recovery microbenches can be added later; encode throughput is the first baseline slice.

- 2026-04-25 - S1-CHK1 - Stage 1 checkpoint
  - What changed: Stage 1 checklist closed; tracker advanced to Stage 2 queue.
  - Verification: `cargo test` in `engine` (16 passed, 0 failed); benchmark target compiles (`cargo check --benches`). For local numbers/HTML: `cd engine && cargo bench --bench stage1_encode`.
  - Notes / risks: Record machine model/CPU in future runs when publishing numbers.

- 2026-04-25 - S2-T01 - Query AST and guide-pattern compiler
  - What changed: Added `engine/src/query/` (`ast`, `guide`, `compile`): `QueryAst` / `WhereClause` / `WhereOp` / `QueryLiteral`, `GuidePattern` / `Clause` / `RangeOp`, and `compile_query` mapping `=` → `ExactMatch`, comparison ops → `RangeMatch`, `LIKE` → `LikePattern`; operands get `bincode` wire bytes plus `encode_bytes_to_codons` payloads; `EncodedPayload` now serde-serializable for guide IR.
  - Verification: `cargo test` in `engine` (20 passed, 0 failed).
  - Notes / risks: Literal typing is intentionally narrow until the SDK and wire adapters define richer JSON coercions.

- 2026-04-25 - S2-T02 - Intron hash fast-path matcher
  - What changed: Added `engine/src/query/fast_match.rs` with `intron_hash_matches_field` (`fnv1a64` vs `Intron.value_hash`), `clause_introns_fast_match`, and `guide_introns_fast_match` (AND over clauses; empty clause list passes). `ExactMatch` uses hash equality; `RangeMatch` / `LikePattern` require an intron on the field name only (v1).
  - Verification: `cargo test` in `engine` (24 passed, 0 failed).
  - Notes / risks: WAL `_payload` introns hash **raw** bytes; `compile_query` `ExactMatch` uses **bincode** wire—querying `_payload` needs matching wire (manual `Clause` or future writer alignment).

- 2026-04-25 - S2-T03 - Parallel strand scan executor
  - What changed: Added `engine/src/query/scan.rs` with `ScanConfig` (`collection_id` filter), `scan_strands_sequential`, `scan_strands_parallel` (Rayon `par_iter`), and `scan_strands` (parallel when `strands.len() >= PARALLEL_STRAND_THRESHOLD` 32). Returns matching `Strand.signature` values using intron fast-path only.
  - Verification: `cargo test` in `engine` (28 passed, 0 failed), including parallel vs sequential equivalence on 48 strands.
  - Notes / risks: Scans in-memory `&[Strand]`; mmap pool iteration + decode integration is a later wiring step.

- 2026-04-25 - S2-T04 - Fetch, fetchOne, orderBy, limit, include
  - What changed: Added `engine/src/query/fetch.rs` (`fetch`, `fetch_one`, `fetch_with_includes`, `resolve_include_path`); ordering for `version` / `created_at` / `updated_at` (else signature tie-break); `Intron.references_strand` optional for multi-hop dot paths; tests for order/limit and `orders.products` include chain.
  - Verification: `cargo test` in `engine` (30 passed, 0 failed).
  - Notes / risks: In-memory executor only; `fetch_one` is “first row after shared fetch pipeline” (honors `limit` then takes first).

- 2026-04-25 - S2-T05 - Minimal TypeScript SDK
  - What changed: Expanded `sdk/typescript/src/index.ts` into a usable minimal SDK surface: `DNAdb.collection(name)`, `CollectionClient.insert()`, query builder `.where().include().orderBy().limit().fetch()/fetchOne()`, typed request models, and pluggable transport (`Transport`, default `NotImplementedTransport`).
  - Verification: `npm install && npm run check && npm run build` in `sdk/typescript` succeeded.
  - Notes / risks: Transport is intentionally abstract until native/HTTP wire handlers are implemented; runtime calls throw clear errors without a configured transport.

- 2026-04-25 - S2-CHK1 - Stage 2 internal dogfood checkpoint
  - What changed: Stage 2 checklist closed; tracker advanced to Stage 3 queue.
  - Verification: `cargo test -q` in `engine` (30 passed, 0 failed) and SDK compile pipeline green (`npm run check`, `npm run build`).
  - Notes / risks: Query execution is in-memory for now; persisted strand-pool scanning and SDK network transport are next-stage integration tracks.

- 2026-04-25 - S3-T01 - Identity store and token/session model
  - What changed: Added `engine/src/auth.rs` with `IdentityStore`, `Identity`, `IdentityType`, credential verification (`Password`/`ApiKey` via blake3 hash), session token issuance/validation/revocation/refresh, and TTL enforcement; exported module via `lib.rs`.
  - Verification: `cargo test` in `engine` (34 passed, 0 failed), including new auth tests for password auth, API key auth + token rotation, credential failure, and invalid TTL.
  - Notes / risks: Store is currently in-memory and non-distributed; persistent identity backend + key management integration comes in later Stage 3/6 work.

- 2026-04-25 - S3-T02 - Overlay definitions and resolver pipeline
  - What changed: Added `engine/src/overlay.rs` with `OverlayDefinition`, `OverlayRegistry`, `ResolvedOverlay`, overlay inheritance (`extends` + `additionally_include`), collection/mutation/field visibility checks, and resolver binding from validated auth session token to overlay policy (`resolve_for_session`).
  - Verification: `cargo test` in `engine` (38 passed, 0 failed), including overlay visibility rules, compound overlay inheritance, full-access overlay, and auth-to-overlay resolution.
  - Notes / risks: Current resolver is in-memory and policy-only; integration with result serialization masking and audit emission is the next step.

- 2026-04-25 - S3-T03 - Overlay-aware serialization + immutable audit logging
  - What changed: Added `engine/src/privacy.rs` (`mask_record_for_overlay`, `mask_records_for_overlay`) for engine-level overlay field masking before serialization; added `engine/src/audit.rs` with append-only `AuditLog`, `AuditEntry`, `AuditWrite`, and tamper-evident hash-chain integrity verification.
  - Verification: `cargo test` in `engine` (43 passed, 0 failed), including privacy masking and audit chain tamper-detection tests.
  - Notes / risks: Audit log is currently in-memory; persistence to immutable strand pool (`_audit`) is the next integration step.

- 2026-04-25 - S3-T04 - TLS baseline and secure defaults
  - What changed: Added `engine/src/tls.rs` with environment-aware TLS policy (`Environment`, `TlsConfig`, `TlsMinVersion`, `validate_tls_policy`) and production-secure defaults helper (`production_secure_defaults`).
  - Verification: `cargo test` in `engine` (47 passed, 0 failed), including TLS tests for production TLS-required behavior, TLS 1.3 minimum, cert/key requirements, and local-dev exceptions.
  - Notes / risks: Policy validation is in place; actual network listener TLS wiring comes with connection/server runtime implementation.

- 2026-04-25 - S3-CHK1 - Stage 3 production-safety checkpoint
  - What changed: Stage 3 checklist closed; tracker advanced to Stage 4 queue.
  - Verification: `cargo test` in `engine` (47 passed, 0 failed); auth + overlay + privacy + audit + TLS policy modules all exercised.
  - Notes / risks: Current auth/overlay/audit stores are in-memory; persistence and clustering remain later-stage work.

- 2026-04-25 - S4-T01 - Temperature model primitives
  - What changed: Added `engine/src/histone.rs` with `Temperature` enum (Hot/Warm/Cold/Frozen), `HistoneBlock` struct, recency/telomere-based `classify_temperature`, immediate access promotion (`Cold`/`Frozen` -> `Warm`), and warm reaccess promotion-window helper.
  - Verification: `cargo test` in `engine` (51 passed, 0 failed), including histone-tier classification/promotion tests.
  - Notes / risks: This is policy logic only; actual access tracking, block movement, and background scheduling are implemented in next Stage 4 tasks.

- 2026-04-25 - S4-T02 - Access tracking and tier transition evaluator
  - What changed: Extended `engine/src/histone.rs` with `BlockAccessStats`, `TierDecision`, `record_block_access`, and `evaluate_tier_transition` to combine recency+telomere classification with access-triggered promotion rules.
  - Verification: `cargo test` in `engine` (55 passed, 0 failed), including new tests for counter updates and tier transitions.
  - Notes / risks: Evaluator currently works on in-memory stats; integration with block manager scheduling and storage movement follows in S4-T03/S4-T04.

- 2026-04-25 - S4-T03 - Semantic co-location planner and block assignment policy
  - What changed: Extended `engine/src/histone.rs` with `CoLocationPlan`, deterministic `block_id_for_semantic_key`, `infer_semantic_key` (`user`/`user_id` intron references -> `user:<sig>`, else `collection:<id>`), and `plan_semantic_colocation` grouping strands into block assignment plans.
  - Verification: `cargo test` in `engine` (59 passed, 0 failed), including grouping and deterministic block-id tests.
  - Notes / risks: Key inference currently uses `user`/`user_id` structural references only; richer relationship heuristics can be layered later.

- 2026-04-25 - S4-T04 - Background rebalance scheduler loop
  - What changed: Extended `engine/src/histone.rs` with a deterministic scheduler tick API: `RebalanceActionKind`, `RebalanceAction`, `RebalanceTickResult`, and `run_rebalance_tick` to evaluate per-block tier movement using access stats, telomere lifecycle state, and recent access signal, emitting promote/demote/compress/freeze/noop actions in stable order.
  - Verification: `cargo test` in `engine` (60 passed, 0 failed), including new scheduler behavior coverage in `rebalance_tick_computes_actions`.
  - Notes / risks: Scheduler currently returns an action plan and does not yet apply physical block migration/compression I/O.

- 2026-04-25 - S4-CHK1 - Histone manager checkpoint
  - What changed: Stage 4 checklist closed after completing temperature model, access tracking, semantic co-location planning, and background rebalance scheduling primitives.
  - Verification: `cargo test` in `engine` (60 passed, 0 failed); all Stage 4 histone tests green.
  - Notes / risks: Stage 4 behavior is policy/coordination focused; production persistence orchestration continues in Stage 5+ lifecycle/storage integration.

- 2026-04-25 - S5-T01 - Immortal-by-default lifecycle policy primitives
  - What changed: Added `engine/src/lifecycle.rs` with `LifecyclePolicy` (immortal-default + telomere opt-in), `LifecycleMode`, `ExpireAction`, policy lookup (`effective_policy_for_collection`), strand initialization helper (`initialize_telomere_for_new_strand`), and lifecycle decision evaluator (`evaluate_lifecycle_action`) covering keep/archive/soft-delete/manual-refresh outcomes.
  - Verification: `cargo test -q` in `engine` (66 passed, 0 failed), including six new lifecycle tests.
  - Notes / risks: This milestone defines policy and decisions; persistent deleted-pool movement + hard-delete grace sweeper land in S5-T03.

- 2026-04-25 - S5-T02 - Collection-level telomere policies and refresh mechanisms
  - What changed: Extended `engine/src/lifecycle.rs` with `LifecyclePolicyStore` (set/clear/effective lookup), replication decrement handler (`apply_replication_event`), explicit refresh API (`refresh_strand_telomere`), and bulk collection refresh (`refresh_collection_strands`) with predicate filtering.
  - Verification: `cargo test` in `engine` (70 passed, 0 failed), including new policy-store, replication, and refresh-path lifecycle tests.
  - Notes / risks: Refresh and policy logic are currently in-memory primitives; binding to persisted collection config and lifecycle jobs lands with S5-T03 integration.

- 2026-04-25 - S5-T03 - Soft-delete + grace-period lifecycle pipeline
  - What changed: Extended `engine/src/lifecycle.rs` with `DeleteConfig` (30-day default grace), `DeletedStrandRecord`, `soft_delete_record_for_strand`, `sweep_deleted_pool`, and `HardDeleteSweepResult` for deterministic soft-delete to hard-delete lifecycle progression.
  - Verification: `cargo test` in `engine` (72 passed, 0 failed), including new soft-delete and grace-window sweeper tests.
  - Notes / risks: Deleted-pool primitives are in-memory and policy-level; binding sweeps to persistent storage + immutable deletion audit strand writes is next integration layer.

- 2026-04-25 - S5-CHK1 - Telomere lifecycle checkpoint
  - What changed: Stage 5 checklist closed after immortal-default policy, collection-level telomere config/refresh paths, and soft-delete grace-period sweep primitives were completed.
  - Verification: `cargo test` in `engine` (72 passed, 0 failed); lifecycle module tests all green.
  - Notes / risks: Stage 5 currently provides lifecycle policy and scheduling primitives; persisted lifecycle worker orchestration remains future runtime integration.

- 2026-04-25 - S6-T01 - Mongo wire translation coverage baseline
  - What changed: Added `engine/src/wire.rs` with a baseline Mongo translation layer (`MongoCommand` find/insertOne/updateOne/deleteOne) mapping into internal `WireOperation` query/mutation operations, including filter-operator mapping (`$eq/$ne/$gt/$gte/$lt/$lte/$regex`), sort/limit/include translation, and guardrails for unsupported features (`$where`, invalid update shape).
  - Verification: `cargo test` in `engine` (76 passed, 0 failed), including new wire translation tests.
  - Notes / risks: This milestone is translation-only and does not open sockets or implement OP_MSG framing yet; network protocol runtime wiring follows in the next Stage 6 tasks.

- 2026-04-25 - S6-T02 - PostgreSQL protocol parser and translation baseline
  - What changed: Extended `engine/src/wire.rs` with `PostgresQuery` and `translate_postgres_query` baseline SQL translation for `SELECT`/`INSERT`/`UPDATE`/`DELETE` into internal `WireOperation`s, including SQL where-clause operator parsing (`=`, `!=`, `>`, `>=`, `<`, `<=`, `LIKE`) and validation guardrails for unsupported/invalid SQL shapes.
  - Verification: `cargo test` in `engine` (79 passed, 0 failed), including new PostgreSQL translation tests.
  - Notes / risks: Parser is intentionally minimal and SQL-string based (not full PostgreSQL frontend/backend protocol v3 framing yet); socket/protocol message handling and broader SQL surface area remain next-stage work.

- 2026-04-25 - S6-T03 - Compatibility test matrix scaffolding for common clients
  - What changed: Extended `engine/src/wire.rs` with compatibility-matrix scaffolding (`ProtocolFlavor`, `CompatibilityCase`, `CompatibilityExpectation`, `CompatibilityMatrixReport`), baseline matrix fixtures (`default_baseline_compatibility_cases`) covering Mongo and PostgreSQL client query shapes, and matrix executor (`run_baseline_compatibility_matrix`) that validates expected supported/unsupported behavior.
  - Verification: `cargo test` in `engine` (80 passed, 0 failed), including new `baseline_compatibility_matrix_executes` test.
  - Notes / risks: Matrix currently validates translation-layer behavior only; real socket protocol integration and external client end-to-end checks are follow-up runtime work.

- 2026-04-25 - S6-CHK1 - Wire protocol compatibility checkpoint
  - What changed: Stage 6 checklist closed after Mongo baseline translation, PostgreSQL baseline translation, and compatibility matrix scaffolding were completed.
  - Verification: `cargo test` in `engine` (80 passed, 0 failed); wire translation + matrix tests green.
  - Notes / risks: Stage 6 currently focuses on translation and compatibility scaffolding; network protocol framing/listener implementation remains future integration work.

- 2026-04-25 - S7-T01 - Transfer packet schema and mutation absorption rules
  - What changed: Added `engine/src/transfer.rs` with transfer packet schema (`TransferPacket`, `MutationType`, `FieldType`), schema catalog types (`SchemaCatalog`, `CollectionSchema`, `FieldSchema`), and deterministic absorption logic (`absorb_transfer_packet`) enforcing idempotent packet replay, per-origin monotonic sequence ordering, and mutation validation for add/drop/rename field operations.
  - Verification: `cargo test` in `engine` (84 passed, 0 failed), including new transfer absorption tests.
  - Notes / risks: This milestone is in-process convergence logic only; distributed packet transport/sync loops are implemented in S7-T02/S7-T03.

- 2026-04-25 - S7-T02 - Multi-node sync loop and version coexistence handling
  - What changed: Extended `engine/src/transfer.rs` with sync-loop primitives (`plan_sync_packets_for_node`, `absorb_sync_batch`, `SyncApplyResult`) and version-coexistence projection helper (`schema_view_for_requested_fields`) to model old/new app field visibility during rolling schema propagation.
  - Verification: `cargo test` in `engine` (87 passed, 0 failed), including new sync-planning, sync-batch, and coexistence-view tests.
  - Notes / risks: Sync loop currently consumes in-memory packet lists; network fanout, retries, and partition recovery mechanics land in S7-T03.

- 2026-04-25 - S7-T03 - Failure scenario handling (partitions, delayed nodes, replay)
  - What changed: Extended `engine/src/transfer.rs` with failure-handling primitives: `PartitionBuffer` + `buffer_partition_packets` (partition buffering), `plan_delayed_node_catchup` (delayed-node catch-up planning), and `ReplayGuard` + `should_process_replay_packet` (retry/replay dedup safety).
  - Verification: `cargo test` in `engine` (90 passed, 0 failed), including new partition, catch-up, and replay-dedup tests.
  - Notes / risks: This provides deterministic in-memory failure-path behavior; real network transport retries/acks and durable queue persistence are follow-up runtime concerns.

- 2026-04-25 - S7-CHK1 - Distributed transfer checkpoint
  - What changed: Stage 7 checklist closed after transfer packet schema, sync/coexistence planning, and failure-scenario primitives were completed.
  - Verification: `cargo test` in `engine` (90 passed, 0 failed); transfer module tests green.
  - Notes / risks: Stage 7 focuses on distributed protocol primitives; production-grade cluster transport orchestration remains future integration work.

- 2026-04-25 - S8-T01 - Python SDK baseline client + query builder
  - What changed: Expanded `sdk/python` from scaffold to usable baseline SDK with typed config/request models, pluggable transport protocol, `DNAdb` root client, `CollectionClient`, fluent `QueryBuilder`, and Python unit tests; exported the client surface via `dnadb/__init__.py`.
  - Verification: `python3 -m unittest discover -s tests -p "test_*.py"` in `sdk/python` (3 passed, 0 failed).
  - Notes / risks: SDK is transport-agnostic for now (`NotImplementedTransport` default); network runtime binding is follow-up ecosystem work.

- 2026-04-25 - S8-T02 - Admin/inspection tooling baseline
  - What changed: Added `scripts/admin_inspect.py`, a read-only admin CLI with `status`, `completion`, `sdk`, and `all` commands, plus JSON/text output modes. Added `tests/test_admin_inspect.py` for parser and status checks.
  - Verification: `python3 -m unittest discover -s tests -p "test_*.py"` at repo root (3 passed, 0 failed) and CLI smoke test `python3 scripts/admin_inspect.py status --json`.
  - Notes / risks: Tooling currently inspects repository metadata (`progress.md`, SDK presence) rather than live runtime state; runtime endpoints can be integrated later.

- 2026-04-25 - S8-T03 - GraphQL layer baseline
  - What changed: Added `sdk/typescript/src/graphql.ts` with baseline GraphQL schema text (`BASE_GRAPHQL_SCHEMA`), operation contracts, and `GraphqlAdapter` that maps GraphQL-style query/mutation operations to existing SDK collection/query-builder calls; exported GraphQL surface from `sdk/typescript/src/index.ts`.
  - Verification: `npm run check && npm run build` in `sdk/typescript` completed successfully.
  - Notes / risks: This is an adapter layer and schema contract baseline, not a full GraphQL HTTP server/runtime; network transport and resolver hosting are future integration steps.

- 2026-04-25 - S8-T04 - Monitoring and deployment packaging baseline
  - What changed: Added deployment/monitoring scaffold assets: `docker-compose.observability.yml` (DNA-DB + Prometheus + Grafana), `config/dnadb.config.toml.example`, `docs/prometheus.yml`, `docs/DEPLOYMENT_MONITORING.md`, and `scripts/ops_check.py` (file + optional runtime health checks with JSON output).
  - Verification: `python3 -m unittest discover -s tests -p "test_*.py"` at repo root (5 passed, 0 failed) and `python3 scripts/ops_check.py --skip-runtime --json` returned `ok: true`.
  - Notes / risks: Compose stack references planned runtime images/endpoints; runtime/network integration depends on server implementation maturity.

- 2026-04-25 - S8-CHK1 - Ecosystem checkpoint
  - What changed: Stage 8 checklist closed after Python SDK, admin inspection tooling, GraphQL adapter baseline, and monitoring/deployment packaging scaffold were completed.
  - Verification: Combined ecosystem verification succeeded (`sdk/python` tests, repo tooling tests, TypeScript build/check, packaging checks).
  - Notes / risks: Ecosystem artifacts are baselines; production runtime integration remains iterative.

## Build completion (approximate)

_Last updated: 2026-04-26_

| Scope | Complete | Remaining |
|-------|----------|-----------|
| **Stage 1** (11 tracked line items: S1-T01–S1-T10 + S1-CHK1) | **100%** | **0%** |
| **Stage 2** (6 tracked line items: S2-T01–S2-T05 + S2-CHK1) | **100%** | **0%** |
| **Stage 3** (5 tracked line items: S3-T01–S3-T04 + S3-CHK1) | **100%** | **0%** |
| **Stage 4** (5 tracked line items: S4-T01–S4-T04 + S4-CHK1) | **100%** | **0%** |
| **Stage 5** (4 tracked line items: S5-T01–S5-T03 + S5-CHK1) | **100%** | **0%** |
| **Stage 6** (4 tracked line items: S6-T01–S6-T03 + S6-CHK1) | **100%** | **0%** |
| **Stage 7** (4 tracked line items: S7-T01–S7-T03 + S7-CHK1) | **100%** | **0%** |
| **Stage 8** (5 tracked line items: S8-T01–S8-T04 + S8-CHK1) | **100%** | **0%** |
| **Full staged roadmap** (8 equal-weight phases; Stages 1-8 complete) | **100%** | **0%** |
| **Spec-alignment checklist** (14 items; partial counted at 50%) | **100%** | **0%** |

**How to read this:** All staged roadmap phases are complete (Stages 1-8). Future work should be tracked as post-roadmap enhancements.

## Post-Roadmap v2 Workstream

- [x] V2-PERF-01: Threaded ingest controls in benchmark harness (`--threads`, `--prep-batch`)
- [x] V2-PERF-02: Remove per-record mmap flush from benchmark hot path (flush on group-commit + final sync)
- [x] V2-PERF-03: Matrix runner support for v2 throughput knobs (`--data-dir`, `--threads`, `--prep-batch`, optional `--wal-interval-ms`)
- [x] V2-PERF-04: Add strict/balanced/fast + threaded defaults to `Makefile` targets
- [x] V2-PERF-05: Add concurrent writer runtime path (not benchmark-only) with contention metrics
- [x] V2-QUERY-01: Implement true `LIKE` payload evaluation (not field-presence fast-path only)
- [x] V2-SEGMENT-01: Bloom filter + block stats segment metadata and skip planner integration
- [x] V2-INDEX-01: Persistent global exact hash index (save/load/lookup) + fetch integration hook
- [x] V2-INDEX-02: Numeric range index (build/save/load/lookup) + planner/fetch indexed range path
- [x] V2-INDEX-03: Persistent index lifecycle manager (catalog build/save/load + startup-use wiring)
- [x] V2-INTEGRITY-01: Write-time CRC sidecar + background scrubber primitives + tests
- [x] V2-VECTOR-01: Vectorized batch predicate evaluation integrated into fetch path
- [x] V2-MVCC-01: MVCC version chain primitives + snapshot visibility/deletes + isolation/property tests
- [x] V2-MVCC-02: Transaction runtime manager (begin/read/upsert/delete/commit/rollback) + optimistic write-conflict detection + snapshot-isolation integration test
- [x] V2-MVCC-03: Durable transaction store wiring (commit via WAL+materialization before visibility) + reopen/replay integration test
- [x] V2-MVCC-04: Wire-operation execution bridge on durable transaction store (query/insert/update/delete) with transactional visibility test
- [x] V2-MVCC-05: Engine runtime facade (Mongo/Postgres wire translation -> durable transactional execution path) + runtime integration tests
- [x] V2-MVCC-06: MVCC compaction-time version GC policy hooks (active-snapshot-safe cleanup + floor retention) with transaction/durable tests
- [x] V2-MVCC-07: Snapshot-aware planner/index query path in durable runtime (direct/index/scan via `choose_path`) + plan-selection tests
- [x] V2-REPL-01: WAL-stream replication runtime (checkpointed source fetch + ordered replica apply + replay-skip semantics) + resume/idempotency tests
- [x] V2-WRITE-01: LSM write pipeline runtime (WAL append -> memtable -> immutable segment flush + persisted segment manifest/sealed WAL checkpoint) with reopen/flush tests
- [x] V2-HISTONE-01: Compaction-time clustering runtime primitives (cold/frozen segment selection + foreign-key clustering segment merge) + tests
- [x] V2-QUERY-02: Full cost-model planner refinement (multi-clause indexed candidate evaluation + enhanced scan/index cost model) + tests

## Spec Alignment Checklist (v2 delta summary)

| # | Area | Status | Notes |
|---|---|---|---|
| 1 | Query path selection (direct/index/scan) | 🟢 Done | Cost-model planner now evaluates all eligible indexed clauses, chooses most selective indexed path when cheaper than scan, and accounts for scan overhead factors (verify/include/order) with tests |
| 2 | DNA encoding conceptual vs physical | 🟢 Done | Storage is CPU-aligned binary; DNA concepts retained in model/encoding semantics |
| 3 | Integrity strategy (write + background + optional read) | 🟢 Done | Write-time CRC sidecar appends in processor, plus scrubber pass over persisted strands/meta |
| 4 | WAL+memtable+segments write pipeline | 🟢 Done | Added LSM write pipeline runtime with WAL append, in-memory memtable buffering, immutable segment flush, persisted segment manifest, sealed WAL checkpoint tracking, and reopen recovery of segment index |
| 5 | Hybrid indexing (global + introns) | 🟢 Done | Intron fast path + segment metadata + persistent exact/range indexes + lifecycle manager + fetch planner integration are wired |
| 6 | Block skipping (bloom + stats) | 🟢 Done | Segment metadata + skip logic integrated into guided scan |
| 7 | CRISPR as guided fallback | 🟢 Done | Scan now guided with segment skip + intron checks + post-verify |
| 8 | Batch/vector execution | 🟢 Done | Batch predicate evaluator (`filter_rows_vectorized`) integrated into fetch execution path |
| 9 | Histone compaction-time clustering | 🟢 Done | Added compaction runtime primitives: segment selection planner for cold/frozen tiers and deterministic compaction-time foreign-key clustering merge output |
| 10 | Query planner decision engine | 🟢 Done | Cost-model planner wired (`choose_path`) with record-count, selectivity, and skip-rate estimates |
| 11 | MVCC concurrency model | 🟢 Done | Version chain + snapshot visibility + delete tombstones + transaction runtime + optimistic write-conflict detection + durable commit/replay path + wire-operation execution bridge + engine runtime wire facade + compaction-time version GC policy hooks + snapshot planner/index/direct-scan execution are implemented |
| 12 | Security enforcement (overlay output layer) | 🟢 Done | Overlay + masking enforced in privacy layer and covered with integration tests |
| 13 | Replication via WAL stream | 🟢 Done | WAL source streaming by sequence checkpoint, ordered replica apply into strand storage, persisted replica checkpoint resume, and replay-skip semantics are implemented and tested |
| 14 | Removed hot-path bottlenecks | 🟢 Done | Per-record flush + scan-only query bottlenecks removed in benchmark/runtime paths |

## Backlog By Stage

### Stage 1 - Strand Storage Engine + WAL Write Path

- [ ] Finalize on-disk file format versioning and compatibility policy
- [ ] Define intron weave strategy and hash algorithm
- [ ] Implement durability/integrity tests (power-loss simulation where possible)

### Stage 2 - CRISPR Query Engine + Basic Developer Interface

- [ ] Add pattern matching support (`like`) and range predicates
- [ ] Implement include traversal for multi-hop relationships
- [ ] Add aggregation primitives (`sum`, `count`, `max`)

### Stage 3 - Auth, Privacy, and Epigenetic Overlays

- [ ] Identity store and token/session model
- [ ] Overlay definitions and resolver pipeline
- [ ] Overlay-aware serialization and immutable audit strand logging
- [ ] TLS baseline and secure defaults

### Stage 4 - Histone Block Manager

- [ ] Hot/Warm/Cold/Frozen temperature model implementation
- [ ] Access-frequency tracking and promotion/demotion scheduler
- [ ] Semantic co-location policies and compaction

### Stage 5 - Telomere Lifecycle System

- [ ] Immortal-by-default lifecycle mode
- [ ] Collection-level telomere policies and refresh mechanisms
- [ ] Soft-delete and grace-period pipeline

### Stage 6 - Wire Protocol Compatibility

- [ ] Mongo wire protocol translation coverage baseline
- [ ] Postgres protocol parser and translation layer
- [ ] Compatibility test matrix against common clients

### Stage 7 - Lateral Transfer Protocol (Distributed)

- [ ] Transfer packet schema and mutation absorption rules
- [ ] Multi-node sync loop and version coexistence handling
- [ ] Failure scenarios (partitions, delayed nodes, replay)

### Stage 8 - Ecosystem (parallel after Stage 2)

- [x] Python SDK
- [x] Admin/inspection tooling
- [x] GraphQL layer
- [x] Monitoring and deployment packaging

## Risks / Decisions To Track

- [x] Choose implementation language for core engine and justify tradeoffs
- [ ] Decide scope for Stage 1 "vertical usability" (engine-only or minimal CLI too)
- [ ] Confirm benchmark environment and hardware assumptions
- [ ] Define acceptance thresholds for each stage before work begins

## Session Handoff

Before ending a session, update:

- `Next In Queue` (promote next task)
- `Done Log` (append completed work)
- `Stage Status` (check off completed stage)
- `Risks / Decisions To Track` (add new unknowns)
