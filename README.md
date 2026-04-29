# DNA-DB

**DNA-DB is a fast document database where privacy, indexing, and flexible querying are built into the data model itself**—so you write less code, avoid entire classes of bugs, and still get strong performance.

## Why DNA-DB is different

1. **Privacy is enforced by the engine**  
   Data returned from the database can be filtered based on identity and permissions. The goal is to eliminate “raw access” paths that accidentally leak sensitive fields—policy lives next to the data, not only in app code.

2. **Indexes live with the data**  
   Each record carries searchable metadata aligned with the stored document. That means no separate index rebuild drift, no “index says one thing, row says another,” and predictable query behavior without constant manual tuning.

3. **Scans are a first-class operation**  
   Many systems punish you when a query doesn’t match a perfect index. DNA-DB is built so flexible, search-like queries stay usable in real APIs—not only narrow key lookups.

## Where DNA-DB shines

- Content systems (CMS, feeds, blogs)
- Multi-tenant apps with strict data visibility rules
- APIs with flexible filtering (search-like queries)
- Systems where schema evolves frequently
- Apps that don’t want to operate a separate search cluster for every product surface

## Where it’s not the best choice

- Heavy analytical **JOIN** workloads across many normalized tables
- Complex relational reporting where mature SQL ecosystems are the default answer
- Situations where you require decades of battle-tested third-party tooling around a traditional RDBMS

## Quick start

Run RC-1 in Docker:

```bash
docker run -d \
  --name dnadb \
  -p 8787:8787 \
  -v dnadb_data:/data \
  dnadb/server:rc-1
```

Verify:

```bash
curl http://localhost:8787/api/health
# {"ok":true,"service":"dnadb_http"}
```

## Examples and SDKs

**Runnable examples** (small Node demos—each has a README):

| Path | What it shows |
|------|-----------------|
| [examples/README.md](examples/README.md) | Index of all examples |
| [examples/quick-demo/](examples/quick-demo/) | One-minute walkthrough |
| [examples/02_privacy_overlays/](examples/02_privacy_overlays/) | Field-level masking at the DB layer |
| [examples/01_no_index_queries/](examples/01_no_index_queries/) | Queries without manual index setup |
| [examples/04_hybrid_queries/](examples/04_hybrid_queries/) | Mixed filter / sort patterns |
| [cms/README.md](cms/README.md) | CMS-style HTTP lab + seed + query timings |

**Client libraries** (HTTP API today):

- TypeScript: [@dnadb/sdk on npm](https://www.npmjs.com/package/@dnadb/sdk)
- Python: [`dnadb-python-sdk` on PyPI](https://pypi.org/project/dnadb-python-sdk/) — `pip install dnadb-python-sdk`, import package `dnadb`

## Insert records (HTTP)

```bash
curl -X POST http://localhost:8787/api/collections/posts/documents/bulk \
  -H "Content-Type: application/json" \
  -d '{
    "documents": [
      {"id": 1, "title": "Hello", "status": "published", "updated_at": 1},
      {"id": 2, "title": "World", "status": "draft", "updated_at": 2}
    ]
  }'
```

## Query (HTTP)

```bash
curl -X POST http://localhost:8787/api/collections/posts/query \
  -H "Content-Type: application/json" \
  -d '{
    "filter": {},
    "sort": {"updated_at": -1},
    "limit": 10
  }'
```

## Configure indexes (HTTP)

```bash
curl -X POST http://localhost:8787/api/collections/posts/configure \
  -H "Content-Type: application/json" \
  -d '{
    "sort_indexes": ["updated_at", "created_at", "price"],
    "exact_string_fields": ["slug", "sku", "email"]
  }'
```

## Privacy in practice (TypeScript SDK)

Same query shape for every caller; the engine applies visibility rules (see [examples/02_privacy_overlays/](examples/02_privacy_overlays/) for a runnable demo):

```ts
// Same query, different callers — masking enforced by the engine / overlays.
const rows = await db.collection("users").where("id", "=", 123).fetch();
```

**Admin might see:**

```json
{ "id": 123, "email": "user@email.com", "role": "admin" }
```

**A regular user might see:**

```json
{ "id": 123, "role": "admin" }
```

No hand-rolled “if role !== admin then delete row.email” in every route—policy stays centralized.

## Flexible queries without a separate search pipeline (TypeScript SDK)

```ts
await db
  .collection("posts")
  .where("status", "=", "published")
  .where("title", "like", "%react%")
  .fetch();
```

Designed for API-style filters and text-style predicates without you wiring a second system for every feature.

## CMS benchmark (100k posts, lab harness)

Reproduced with `cms/scripts/seed.mjs` + `cms/scripts/bench_queries.mjs` (release `dnadb_http`, durable path, fresh data dir). See [cms/README.md](cms/README.md) and [BENCHMARKING_RUNBOOK.md](BENCHMARKING_RUNBOOK.md).

| Phase | Result |
|--------|--------|
| **Ingest** | ~**46k docs/s** wall for 100k posts (bulk HTTP + index rebuild in the same run) |
| **Point lookup** (`id`) | ~**0.22 ms** avg |
| **Filtered + sort** | ~**0.57 ms** avg |
| **Filtered, limit only** | ~**0.52 ms** avg |
| **Prefix-style `LIKE`** | ~**1.5 ms** avg |
| **Contains / scan-style** | ~**0.76 ms** avg |

**In plain terms:** typical CMS-style API queries land **under ~2 ms** on this matrix at 100k rows, without per-query index babysitting.

*(First request after cold start can spike; see runbook for warm-up and larger corpora.)*

## Performance (RC-1 and validated paths)

- Write throughput: ~**40k docs/s** (HTTP, single node, durable path)
- Query matrix: sub-**2 ms** class behavior on CMS-style workloads at **100k** (above); **1M**-scale numbers in [BENCHMARKING_RUNBOOK.md](BENCHMARKING_RUNBOOK.md)
- Crash durability: **20/20** SIGKILL recovery loop (documented in the runbook)

## Environment variables

For `dnadb_http`:

- `DNADB_HTTP_MAX_CONNECTIONS` — max in-flight request slots (default 256)
- `DNADB_SLOW_QUERY_MS` — slow-query warning threshold (default 25)
- `DNADB_AUTH_REQUIRED=1` — require bearer auth for API routes
- `DNADB_TLS_CERT` / `DNADB_TLS_KEY` — TLS certificate and private key paths
- `DNADB_ADMIN_API_KEY` — enables `/api/admin/*` (see engine help / runbook)

Docker image defaults:

- Data dir `/data`
- Bind `0.0.0.0:8787`
- Command: `dnadb_http --data-dir /data --bind 0.0.0.0:8787`

## Docker Compose

```bash
docker compose up -d
curl http://localhost:8787/api/health
```

`docker-compose.yml` is included and mounts persistent data at `/data`.

## Troubleshooting

**Port already in use**

```bash
docker run -p 8788:8787 ...   # remap to any free port
```

**Docker daemon not running**

```bash
# macOS
open -a Docker

# Linux
sudo systemctl start docker
```

**Container exits immediately**

```bash
docker logs dnadb-rc1   # check startup errors
```

**Data not persisting between restarts**

```bash
# Use a named volume, not a local path
docker run -v dnadb_data:/data ...   # ✅ named volume
docker run -v ./data:/data ...       # ⚠️ path binding — permissions may vary
```

## Benchmarking + validation runbook

Use [BENCHMARKING_RUNBOOK.md](BENCHMARKING_RUNBOOK.md) for copy-paste commands:

- Seed throughput runs
- Query matrix benchmarks (including 1M when you need it)
- Crash loop validation
- Test / build gate

## License

MIT
