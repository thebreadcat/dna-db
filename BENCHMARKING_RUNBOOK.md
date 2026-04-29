# Benchmarking and Validation Runbook

This runbook is the handoff doc for reproducing our current performance and crash-safety checks on the **single durable write path**.

## Scope and assumptions

- Raw journal / materializer paths are removed.
- Benchmark target is `dnadb_http` on `127.0.0.1:8787` unless overridden.
- Seed + query scripts live in `cms/scripts`.

## 1) One-time setup

From repo root:

```bash
cd engine
cargo build --release --features http_server --bin dnadb_http
cd ../cms
npm install
```

## 2) Start server (fresh data dir)

From repo root:

```bash
rm -rf /tmp/dnadb-bench
./engine/target/release/dnadb_http --data-dir /tmp/dnadb-bench --bind 127.0.0.1:8787
```

Keep this terminal running. Use another terminal for the commands below.

## 3) Seed throughput benchmark (HTTP bulk ingest)

From `cms/`:

```bash
DNADB_URL=http://127.0.0.1:8787 \
SEED_TOTAL=100000 \
SEED_BATCH=10000 \
npm run seed
```

Capture these from output:

- `Done: ... (N docs/s)` -> wall throughput
- `Sum of server batch ms: ...`
- `rebuild_indexes server_ms=... client_ms=...`

Optional larger validation:

```bash
DNADB_URL=http://127.0.0.1:8787 \
SEED_TOTAL=1000000 \
SEED_BATCH=10000 \
npm run seed
```

## 4) Query benchmark matrix

From `cms/`:

```bash
DNADB_URL=http://127.0.0.1:8787 \
SEED_TOTAL=100000 \
ITERS=30 \
npm run bench
```

This runs:

- `latest_50`
- `published_ordered`
- `published_limit_only`
- `point_id`
- `exact_slug`
- `prefix_slug_like`
- `contains_slug_scan`

Record `avg_ms`, `p95_ms`, `max_ms`, and `qps` from the JSON output.

## 5) Engine-level durable fsync benchmark

From `engine/`:

```bash
cargo run --release --bin bench_durable_bulk -- 100000 20 1
```

This compares:

- strict (`sync_data` each commit)
- deferred + finalize (`flush_maps` each commit, one sync at end)

Capture summary lines:

- `strict wall: ...`
- `defer+finalize: ...`
- `speedup (strict / defer): ...`

## 6) Full test gate

From `engine/`:

```bash
cargo test
```

For release-server sanity:

```bash
cargo build --release --features http_server --bin dnadb_http
```

## 7) 20x SIGKILL crash loop (durability proof)

From repo root:

```bash
killall dnadb_http 2>/dev/null || true
for i in {1..20}; do
  DIR="/tmp/crash-loop-$i"
  rm -rf "$DIR"

  ./engine/target/release/dnadb_http --data-dir "$DIR" --bind 127.0.0.1:8787 >/tmp/dnadb-crash-loop-$i.log 2>&1 &
  SERVER_PID=$!
  sleep 1

  (cd cms && DNADB_URL=http://127.0.0.1:8787 SEED_TOTAL=10000 SEED_BATCH=1000 npm run seed >/tmp/dnadb-seed-loop-$i.log 2>&1)

  kill -9 "$SERVER_PID" 2>/dev/null || true
  sleep 1

  ./engine/target/release/dnadb_http --data-dir "$DIR" --bind 127.0.0.1:8787 >/tmp/dnadb-crash-loop-reopen-$i.log 2>&1 &
  SERVER_PID=$!
  sleep 2

  TOTAL=$(curl -s -X POST http://127.0.0.1:8787/api/collections/posts/query \
    -H "Content-Type: application/json" \
    -d '{"filter":{},"limit":10000}' \
    | jq '.rows | length' 2>/dev/null)

  kill -9 "$SERVER_PID" 2>/dev/null || true

  if [ "$TOTAL" = "10000" ]; then
    echo "✅ Run $i: $TOTAL records — PASS"
  else
    echo "❌ Run $i: $TOTAL records — FAIL (expected 10000)"
  fi
done
```

Expected pass condition: **20/20 green lines**.

## 8) Artifacts to save in handoff

At minimum, include:

- Seed run command + final throughput line + sum server ms.
- Query bench JSON blob (`npm run bench` output).
- `bench_durable_bulk` summary.
- `cargo test` pass summary.
- 20-line crash loop PASS/FAIL output.

## 9) Common issues

- `Address already in use` on `8787`: kill old process (`killall dnadb_http`) or change `--bind`.
- Missing `jq`: install it or parse JSON manually.
- Slow first query after restart: run a warm-up query or rely on prewarm (`--prewarm-indexes` defaults true).
