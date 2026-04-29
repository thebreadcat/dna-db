# DNA-DB

A biologically-inspired database with fast durable writes, sub-millisecond query paths, built-in text search, and structural privacy controls.

## Quick Start

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

## Insert Records

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

## Query

```bash
curl -X POST http://localhost:8787/api/collections/posts/query \
  -H "Content-Type: application/json" \
  -d '{
    "filter": {},
    "sort": {"updated_at": -1},
    "limit": 10
  }'
```

## Configure Indexes

```bash
curl -X POST http://localhost:8787/api/collections/posts/configure \
  -H "Content-Type: application/json" \
  -d '{
    "sort_indexes": ["updated_at", "created_at", "price"],
    "exact_string_fields": ["slug", "sku", "email"]
  }'
```

## Environment Variables

For `dnadb_http`:

- `DNADB_HTTP_MAX_CONNECTIONS` max in-flight request slots (default 256)
- `DNADB_SLOW_QUERY_MS` slow-query warning threshold (default 25)
- `DNADB_AUTH_REQUIRED=1` require bearer auth for API routes
- `DNADB_TLS_CERT` TLS certificate path (enables HTTPS when set with key)
- `DNADB_TLS_KEY` TLS private key path

Docker image defaults:

- data dir `/data`
- bind `0.0.0.0:8787`
- container command: `dnadb_http --data-dir /data --bind 0.0.0.0:8787`

## Performance (validated on RC-1)

- Write throughput: ~40k docs/s (HTTP, single node, durable path)
- Point lookup: ~0.21ms average
- Filtered sort: ~0.53ms average
- Contains search: ~1.1ms average at 1M records
- P99 ingest latency: ~5.5ms at 1M records concurrent
- Crash durability: 20/20 SIGKILL recovery loop passing

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

## Benchmarking + Validation Runbook

Use `BENCHMARKING_RUNBOOK.md` for exact commands to reproduce:

- seed throughput runs
- query matrix benchmarks
- crash loop validation
- test/build gate

## License

MIT
