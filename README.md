# DNA-DB

DNA-DB is a high-performance experimental database that combines:

* ⚡ LSM-style durability (WAL + segments)
* 🧠 Embedded indexing (no separate index tables)
* 🔍 Hybrid query execution (index + guided scans)
* 🔐 Built-in field-level privacy controls

It explores a different tradeoff space than traditional databases:
**faster writes, flexible querying, and simpler data modeling** — with scan-based execution when indexes aren’t available.

👉 Think: a hybrid between RocksDB, MongoDB, and a columnar scan engine.

---

## Why DNA-DB?

Traditional databases force you to choose:

* Predefine indexes or suffer slow queries
* Add privacy in application code
* Migrate schemas carefully over time

DNA-DB takes a different approach:

* **Indexes live with the data** (embedded “introns”)
* **Queries don’t fail without indexes** — they fall back to fast parallel scans
* **Privacy is enforced at the engine level**, not in app logic
* **Append-first storage** simplifies writes and scaling

This makes DNA-DB well-suited for:

* event-heavy systems
* semi-structured or evolving schemas
* analytics-style workloads
* systems needing built-in privacy controls

---

## Quick Start (5 Minutes)

### 1. Clone + verify

```bash
git clone https://github.com/thebreadcat/dna-db
cd dna-db

make test-all-quick
```

---

### 2. Run a simple example

```bash
cd examples/basic
npm install
node index.js
```

Then open [http://localhost:4173](http://localhost:4173) to view the local dashboard with:
- adaptor query examples (TypeScript, Postgres, GraphQL, Mongo-style),
- sample response payloads,
- benchmark snapshots loaded from `bench-output/*.json`.

If `4173` is already in use, the demo automatically tries the next available port and prints the chosen URL.

---

### 3. Insert + query

```ts
const db = new DNAdb();

await db.collection("users").insert({
  name: "Alice",
  age: 30,
  region: "us-west"
});

const users = await db
  .collection("users")
  .where("age", ">", 25)
  .where("region", "=", "us-west")
  .orderBy("age", "desc")
  .limit(10)
  .fetch();

console.log(users);
```

---

## Where DNA-DB Shines

* 🚀 High write throughput (append-first + batching)
* 🔄 Flexible queries without strict schema planning
* 🔍 Hybrid execution (index + scan fallback)
* 🔐 Built-in privacy overlays (field-level masking)
* 🧩 Embedded indexing (no index/table drift)

---

## Tradeoffs

DNA-DB is not a universal replacement for traditional databases.

* Large full scans can be expensive if not guided
* Query planner is still evolving (edge cases may not be optimal)
* Append-first storage requires compaction tuning
* Ecosystem and tooling are early-stage

👉 If you need strict relational guarantees or highly predictable indexed queries at all times, a traditional database may still be a better fit.

---

## How It Works (Simplified)

DNA-DB uses a layered model:

* **Strands** → stored records (append-only)
* **Introns** → embedded index metadata inside each record
* **WAL** → ensures durability and recovery
* **Query engine**:

  * uses indexes when available
  * falls back to guided parallel scans when not
* **Compaction (Histones)** → reorganizes and clusters data over time
* **Overlays (Epigenetics)** → enforce field-level visibility

---

## Architecture (Deeper View)

DNA-DB is built in layers:

1. Strand storage engine (core persistence)
2. WAL + SIMD write pipeline (durability + speed)
3. Query engine (index + guided scan hybrid)
4. Block manager (compaction + clustering)
5. Overlay system (privacy + schema views)
6. Auth + identity + audit
7. Lifecycle management (retention + archival)
8. Replication + transfer
9. Wire protocol compatibility (Mongo + Postgres)
10. SDKs and ecosystem

Long-form design notes (`DNADB_SPEC.md`, `progress.md`, bottleneck notes, etc.) are intentionally **not** in the public tree; clone maintainers keep them locally and they are listed in `.gitignore`.

---

## Example Query (TypeScript SDK)

```ts
const results = await db
  .collection("orders")
  .where("total", ">", 100)
  .where("status", "=", "completed")
  .include("user.profile")
  .orderBy("created_at", "desc")
  .limit(20)
  .fetch();
```

---

## Benchmarking DNA-DB

DNA-DB includes a built-in benchmarking harness.

### Run the real DB gate

```bash
make test-real-db-gate
```

This validates:

* durability + crash recovery
* MVCC correctness
* index consistency
* compaction safety
* planner adaptivity
* performance baseline

---

### Run load benchmark

```bash
cd engine
cargo run --release --bin load_bench -- --records 100000
```

### Run Docker benchmark (Mongo-style comparable setup)

Use the containerized runner to pin CPU/RAM and compare against Docker-based MongoDB runs:

```bash
THREADS=16 \
PREP_BATCH=10000 \
CONCURRENT_WRITERS=1 \
WAL_SHARDS=16 \
MODE=balanced \
RECORDS=1000000 \
READ_SAMPLE=10000 \
MMAP_BYTES=2634217728 \
DOCKER_CPUS=8 \
DOCKER_MEMORY=16g \
bash scripts/run_load_bench.sh
```

The script writes JSON reports to `bench-output/` and runs `load_bench` inside a Linux container with fixed limits (`--cpus`, `--memory`, `--pids-limit`, `--ulimit`).

---

### Heavy runs (stress testing)

```bash
python3 scripts/test_real_db_gate.py --include-heavy
```

⚠️ Large runs (10M–100M records) are **intentionally slow** and designed for stress testing.

---

## Performance Notes

DNA-DB performance depends on workload:

* writes: optimized (append-first + batching)
* reads:

  * fast with index hints
  * slower but scalable with parallel scans
* large datasets:

  * require compaction tuning
  * benefit from planner improvements

---

## Project Layout

```text
.
├── engine/
├── sdk/
├── tests/
├── benchmarks/
├── examples/
├── scripts/
└── config/
```

---

## Local Development

### Prerequisites

* Rust (stable)
* Node.js 20+
* Python 3.11+

### Setup

```bash
cd engine && cargo check
cd ../sdk/typescript && npm install && npm run build
cd ../python && python -m pip install -e .
```

---

## Testing

```bash
make test-all-quick
make test-all
make test-real-db-gate
```

---

## Security Model

DNA-DB enforces security at multiple levels:

* TLS transport (production default)
* identity + authentication
* field-level overlay masking
* optional at-rest encryption
* immutable audit logs

---

## Compatibility Goals

* Mongo ecosystem (drivers + tools)
* Postgres ecosystem (SQL clients)
* Native SDKs (TypeScript, Python)

---

## Current Status

* Core architecture: ✅ complete
* Spec alignment: ✅ complete (v2)
* Validation: ongoing (stress + soak testing)
* Production readiness: **experimental**

---

## When Should You Use DNA-DB?

Use it if you want to:

* explore a new database model
* build high-write or flexible-schema systems
* experiment with hybrid scan/query execution
* embed privacy directly into your data layer

---

## When Should You Not Use It?

Avoid it (for now) if you need:

* strict relational guarantees
* mature production ecosystem
* predictable query performance in all cases
* enterprise-grade support

---

## Roadmap

* planner improvements
* query optimization
* replication hardening
* ecosystem + tooling expansion

Roadmap detail lives in issues and local planning notes (not in git).

---

## Philosophy

DNA-DB is an experiment in rethinking how databases work:

* less rigid structure
* more adaptive querying
* built-in privacy
* simpler mental model for evolving data

---

## License

TBD

---

## Contributing

1. Open an issue or PR with a focused change
2. Implement with tests
3. Keep changes scoped
4. run verification:

```bash
make test-all-quick
```

---

## Final Note

DNA-DB is not trying to replace existing databases.

It’s exploring a different design space — one where:

* data is flexible
* queries adapt
* privacy is built-in
* and the system stays simple under growth

---

If that direction interests you, try it, break it, and help push it forward 🚀
