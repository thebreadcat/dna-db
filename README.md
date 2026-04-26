# DNA-DB

DNA-DB is a biologically-inspired database engine that uses strand primitives, complement verification, embedded intron indexes, and overlay-based privacy controls.

This repository is the implementation workspace for `DNADB_SPEC.md`, executed phase-by-phase with tracked delivery in `progress.md`.

## Status

- Spec source of truth: `DNADB_SPEC.md`
- Execution tracker: `progress.md`
- Current build stage: Stage 8 (Ecosystem); Stages 1-7 complete in `progress.md`
- Maturity: pre-alpha / actively under construction

## Project Layout

```text
.
├── DNADB_SPEC.md            # complete architecture and build specification
├── progress.md              # active work queue + done log
├── README.md                # this file
├── engine/                  # core database engine (Rust)
├── sdk/
│   ├── typescript/          # TypeScript SDK
│   └── python/              # Python SDK
├── tests/                   # integration/e2e test suites
├── benchmarks/              # perf harnesses (latency/throughput/recovery)
├── docs/                    # architecture and operational docs
├── examples/                # runnable client examples
├── config/                  # default and environment-specific configs
└── scripts/                 # local dev + CI helper scripts
```

## Architecture At A Glance

DNA-DB is built in layers (from `DNADB_SPEC.md`):

1. Strand storage engine (core persistence)
2. WAL + SIMD write pipeline (durability + speed)
3. CRISPR query engine (pattern-first parallel scans)
4. Histone block manager (tiering + co-location)
5. Epigenetic overlays (schema/context views)
6. Connection/auth/privacy (identity + security)
7. Telomere lifecycle (retention and archival behavior)
8. Lateral transfer (schema evolution across nodes)
9. Wire protocol compatibility (Mongo + Postgres)
10. SDKs and ecosystem

## Technology Choices

- Core engine: Rust (memory safety, performance, predictable concurrency)
- TypeScript SDK: Node + ESM package
- Python SDK: `pyproject.toml` package layout
- Storage model: memory-mapped collection files + WAL
- Security model: TLS + identity-to-overlay enforcement + optional at-rest encryption

## Local Development

### Prerequisites

- Rust stable toolchain (recommended via `rustup`)
- Node.js 20+ and npm
- Python 3.11+

### Setup

```bash
# from repo root
cd /path/to/dnadb

# engine
cd engine && cargo check

# TypeScript SDK
cd ../sdk/typescript && npm install && npm run build

# Python SDK
cd ../python && python -m pip install -e .
```

### Repository Workflow

1. Pick next task from `progress.md` (`## Next In Queue`)
2. Implement task with tests/verification
3. Update `progress.md` done log and next item
4. Keep changes scoped to one task when possible

### One-Command Verification

Run the unified verifier from repo root:

```bash
python3 scripts/test_everything.py --quick
```

- `--quick`: skips live runtime endpoint checks (good for local/CI preflight)
- Full mode (no `--quick`) also runs runtime endpoint checks via `scripts/ops_check.py`

Makefile shortcuts:

```bash
make test-all-quick
make test-all
```

A CI workflow (`.github/workflows/verify.yml`) runs the quick verifier on pushes and pull requests.

## Load Testing (100k Records)

DNA-DB now includes a reproducible load benchmark harness:

- Rust benchmark binary: `engine/src/bin/load_bench.rs`
- Container image spec: `Dockerfile.bench`
- Container runner script: `scripts/run_load_bench.sh`

### Local benchmark run

```bash
cd engine
cargo run --release --bin load_bench -- --records 100000 --read-sample 10000 --data-dir ./bench-data-local-100k
```

### Docker benchmark run

```bash
./scripts/run_load_bench.sh
```

Optional environment overrides:

- `RECORDS` (default `100000`)
- `READ_SAMPLE` (default `10000`)
- `MMAP_BYTES` (default `536870912`)
- `MODE` (`strict`, `balanced`, `fast`; default `strict`)
- `BATCH_SIZE` (default `1000`; group-commit batch for `strict` / `balanced`; informational for `fast`)
- `WAL_INTERVAL_MS` (optional; `0` or unset = **batch-only** group commit. Set e.g. `10` on fast local disks to cap how long data may sit unsynced; on slow/network paths a short value can force extra fsyncs)
- `THREADS` (default `1`; prep workers, and writer workers when `--concurrent-writers` is used)
- `PREP_BATCH` (default `5000`; records prepared per chunk before commit)

### Compare durability/performance modes

```bash
python3 scripts/bench_matrix.py --records 100000 --read-sample 10000
# optional: persist JSON + markdown table
python3 scripts/bench_matrix.py --records 100000 --save
# high-throughput threaded matrix on local storage
python3 scripts/bench_matrix.py --records 100000 --threads 16 --prep-batch 10000 --data-dir /tmp/dnadb-bench --save
# true concurrent writer path + contention metrics
python3 scripts/bench_matrix.py --records 100000 --threads 16 --concurrent-writers --data-dir /tmp/dnadb-bench --save
```

Each matrix invocation uses a fresh `bench-data-matrix-<timestamp>/{strict,balanced,fast}` tree so reruns do not append to prior WAL/strand files (strict correctness checks stay valid).

Modes:

- `strict`: **Group commit** — append + mmap flush each record; **one** WAL + storage `fsync` every `--batch-size` records (default `1000`), plus a final sync. Optional `--wal-interval-ms` adds a max **staleness** timer from the first unsynced append in a group (use e.g. `10` on NVMe; default in the harness is timer **off** so slow folders are not penalized).
- `balanced`: same as `strict` in the harness (both use group commit); pick different `--batch-size` / `--wal-interval-ms` for experiments.
- `fast`: no mid-loop fsync (maximum throughput for the harness); a final sync runs before the read phase so the benchmark can reopen storage.

### Output

The benchmark prints JSON with:

- `write_seconds`, `write_records_per_sec`, `wal_interval_ms` (effective timer; 0 = disabled)
- ingest knobs in report payload: `threads`, `prep_batch`
- concurrent-write metrics: `concurrent_writers`, `lock_wait_ms_total`, `lock_hold_ms_total`, `sync_events`
- `decode_seconds`, `decode_records_per_sec`
- point-read sample timing (`point_read_seconds`, `point_reads_per_sec`)
- exact query timing (`query_seconds`, `query_found`)
- full scan timing (`full_scan_seconds`, `full_scan_hits`)
- validation counts (`wal_entries`, `decoded_strands`)
- scan scaling helpers: `negative_exact_scan_seconds` (full parallel pass, zero hits), `*_s_per_million_strands` (normalize query / full scan / decode vs dataset size for 100k vs 1M comparisons)

Notes:

- Per-record fsync (`--batch-size 1` with `--wal-interval-ms 0` in `strict`) matches the old “fsync every append” harness and is useful only for regression against durability cost.
- Compare runs with the same machine/profile and data-dir isolation for meaningful speed tracking.

## Running / Hosting DNA-DB

DNA-DB runtime implementation is in progress. The target runtime model is:

- Native protocol: port `4737`
- Mongo protocol compatibility: port `27017`
- Postgres protocol compatibility: port `5432`
- REST: ports `8080` (HTTP) and `8443` (HTTPS)

### Planned Docker Hosting Model

```bash
docker run -d \
  -p 4737:4737 \
  -p 27017:27017 \
  -p 5432:5432 \
  -p 8080:8080 \
  -p 8443:8443 \
  -v ./dnadb-data:/data \
  -v ./config/dnadb.config.toml:/etc/dnadb/config.toml \
  dnadb/server:latest
```

### Planned Bare-Metal Hosting Model

```bash
dnadb-server --config /etc/dnadb/config.toml
```

## Querying DNA-DB

The primary developer model is fluent document-style querying with SQL-like expressiveness.

```ts
const users = await db
  .collection("users")
  .where("age", ">", 25)
  .where("region", "=", "us-west")
  .orderBy("created_at", "desc")
  .limit(20)
  .fetch();
```

Current TypeScript SDK surface available now in `sdk/typescript`:

- `db.collection("users").insert({...})`
- `db.collection("users").where("age", ">", 25).fetch()`
- `...fetchOne()`, `.include("orders.items")`, `.orderBy(...)`, `.limit(...)`
- Transport is pluggable (`Transport` interface); default transport intentionally throws until wire/runtime adapters are wired.

### Planned Query Features

- Exact match, range, and pattern (`like`) queries
- Include traversal for relationships (`include("orders.items")`)
- Aggregations (`sum`, `count`, `max`)
- Overlay-aware result masking before serialization

## Security and Privacy Model

DNA-DB enforces security/privacy at multiple layers:

1. Transport security (TLS 1.3 by default in production)
2. Authentication and identity binding
3. Overlay-based field-level visibility (engine-level masking)
4. Optional at-rest encryption hierarchy (master -> collection -> strand)
5. Immutable audit logging for reads/writes/deletes/auth events

Current Stage 3 auth foundation in `engine`:

- In-memory identity registry (`IdentityStore`) with identity type + overlay assignment
- Credential verification for password and API key modes (hashed)
- Session token lifecycle: issue, validate, revoke, and rotate (refresh)
- Token TTL enforcement with short-lived session model
- Overlay policy resolver (`overlay` module): define overlays, inherit/compose overlay rules, and resolve auth session -> overlay policy for collection/mutation/field visibility decisions
- Overlay-aware serialization (`privacy` module): mask fields by collection according to resolved overlay before returning records
- Immutable audit primitives (`audit` module): append-only audit entries with hash-chain integrity checks for tamper evidence
- TLS policy baseline (`tls` module): production-safe config validation (TLS required, TLS 1.3 minimum, cert/key presence) with explicit local-dev exceptions
- Histone temperature primitives (`histone` module): Hot/Warm/Cold/Frozen classification helpers and access-promotion rules for Stage 4 storage-tier manager work
- Histone access tracking evaluator (`histone` module): access counters + tier transition decisions (`record_block_access`, `evaluate_tier_transition`)
- Histone semantic co-location planner (`histone` module): semantic-key inference (`user`/`user_id` refs), deterministic `block_id` derivation, and grouping planner for related-strand block assignment
- Histone rebalance scheduler tick (`histone` module): `run_rebalance_tick` emits deterministic promote/demote/compress/freeze/noop actions from block stats + lifecycle state
- Lifecycle policy primitives (`lifecycle` module): immortal-by-default policy, collection-level telomere opt-in policy store/lookup, replication decrement behavior, explicit refresh (`refresh_strand_telomere`), bulk refresh (`refresh_collection_strands`), lifecycle action evaluation (`keep`/`archive`/`soft-delete`), and soft-delete grace pipeline (`DeletedStrandRecord`, `sweep_deleted_pool`)
- Wire compatibility baseline (`wire` module): Mongo command translation (`find`/`insertOne`/`updateOne`/`deleteOne`) into internal operations with filter operator mapping and unsupported-operator guardrails (e.g., `$where`)
- PostgreSQL translation baseline (`wire` module): SQL string translation for `SELECT`/`INSERT`/`UPDATE`/`DELETE` into internal operations with baseline `WHERE` operator parsing and unsupported-statement guardrails
- Wire compatibility matrix scaffolding (`wire` module): baseline cross-protocol case suite + report executor to validate expected supported/unsupported client query shapes
- Lateral transfer schema baseline (`transfer` module): packet schema for field mutations plus deterministic absorption rules (idempotent replay, per-node sequence ordering, add/drop/rename validation)
- Lateral transfer sync/coexistence baseline (`transfer` module): per-node sync planning + batch absorption and schema coexistence views so old/new app versions can safely request different field sets during rollout
- Lateral transfer failure-path baseline (`transfer` module): partition buffering, delayed-node catch-up planning, and replay dedup guardrails for retried packets
- Python SDK baseline (`sdk/python`): config models, transport abstraction, `DNAdb` client, collection/query builder API, and unit-tested fluent query request construction
- Admin/inspection tooling baseline (`scripts/admin_inspect.py`): read-only CLI for stage status, build completion table extraction, SDK presence checks, and JSON output for automation
- GraphQL layer baseline (`sdk/typescript`): schema contract constant plus adapter (`GraphqlAdapter`) that maps GraphQL-style operations onto the existing TypeScript SDK query/mutation surface
- Monitoring/deployment packaging baseline: observability compose stack (`docker-compose.observability.yml`), runtime config template (`config/dnadb.config.toml.example`), Prometheus scrape config (`docs/prometheus.yml`), and operational packaging check command (`scripts/ops_check.py`)

### Security Principles

- Deny-by-default access
- No plaintext transport in production
- No bypass path around overlay masking
- Structured audit trails for compliance and incident response

## Configuration

The main runtime file is `dnadb.config.toml`.

### Key Settings (planned)

- `server.*`: listeners, ports, connection limits
- `auth.*`: token expiry, auth requirements, plaintext allowance
- `tls.*`: cert/key paths and protocol minimums
- `lifecycle.*`: immortal mode, archive/delete controls, grace periods
- `performance.*`: WAL fsync behavior, SIMD, thread counts, batching
- `encryption.*`: at-rest settings and key providers
- `audit.*`: audit enablement and retention behavior
- `replication.*`: cluster nodes and sync mode
- `compatibility.*`: Mongo/Postgres protocol toggles

See `DNADB_SPEC.md` for the full reference baseline until runtime config docs are finalized.

## Engine API (Stage 1 foundation, shipped)

Rust crate `dnadb-engine` (`engine/`) currently exposes:

- **WAL** (`wal::Wal`): durable append with monotonic sequence numbers and full entry replay for recovery.
- **WAL processor** (`processor`): encode raw payload → codons → complement → intron stub → append to `.strands` / `.complement`; optional background worker (`WalProcessorHandle`).
- **Recovery** (`recovery::replay_pending_wal_after_open`): on startup, scan the strand pool for the highest materialized WAL sequence (`Strand.version`), then materialize any WAL entries with a greater sequence (matches the spec’s crash-recovery outline).
- **Encoding** (`encoding`): `encode_bytes_to_codons` (x86_64 AVX2 fast path when available, threshold 64 bytes + scalar fallback), `encode_bytes_to_codons_scalar`, and `encode_bytes_to_codons_avx2` for benchmarking.
- **Query compiler** (`query`): `QueryAst` / `WhereClause` → `compile_query` → `GuidePattern` / `Clause` (exact, range, `LIKE` stub); **fast match** (`intron_hash_matches_field`, `clause_introns_fast_match`, `guide_introns_fast_match`) prefilter introns with `fnv1a64` before full decode; **scan** (`scan_strands`, `scan_strands_parallel`, Rayon) partitions strand lists across CPU cores (v1: in-memory `&[Strand]`); **fetch** (`fetch`, `fetch_one`, `fetch_with_includes`, `resolve_include_path`) applies `orderBy` / `limit` and dot-separated **include** walks via `Intron.references_strand`.

Run engine tests from repo root:

```bash
cd engine && cargo test
```

Stage 1 encode benchmarks (Criterion; HTML under `engine/target/criterion/` after a run):

```bash
cd engine && cargo bench --bench stage1_encode
```

## Testing and Benchmarking

Testing and benchmarking are developed stage-by-stage:

- Unit tests in `engine` for codon encoding, complement verification, and WAL logic
- Integration tests in `tests/` for write/read/recovery behavior
- Benchmark suites in `benchmarks/` for:
  - write latency
  - throughput under load
  - crash recovery replay time
  - query performance by dataset size/pattern specificity

## Compatibility Goals

- Mongo ecosystem: Mongoose, Prisma Mongo adapter, official drivers/tooling
- Postgres ecosystem: `pg`, SQLAlchemy, psycopg, GUI clients
- DNA-DB native SDKs: TypeScript first, Python second

## Roadmap Execution Rule
No skipping stages. Each stage must:
- pass its acceptance criteria
- have reproducible verification notes
