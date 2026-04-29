# Release Candidate Checklist

Status legend: `[ ]` pending, `[-]` in progress, `[x]` done.

## Blockers (Cannot Ship Without These)

- [x] **1. Prefix search superlinearity**
  - Current: ~9ms @ 50k -> ~59ms @ 100k
  - Target: sub-2ms, near-flat scaling
  - Plan: add sorted prefix structure/range lookup for indexed string fields
  - Result (validated): 0.400ms @ 50k -> 0.641ms @ 100k (`prefix_slug_like`)
  - Implemented: bounded prefix range scan + LIMIT-aware early termination + top-N id path

- [x] **2. Single durable write path (raw journal removed)**
  - All ingest goes through `DurableTransactionStore` / `commit_inner` (WAL + strands + MVCC + indexes).
  - Raw journal, materializer, and `write_pipeline` were deleted as redundant after crash-safety required full durable IO on apply.

- [x] **3. Crash recovery test suite**
  - Crash-and-restart validation (`std::mem::forget` simulates abrupt teardown)
  - Verify record completeness and queryability
  - Verify index correctness (sort + exact + trigram)
  - Implemented tests:
    - `crash_recovery_basic`
    - `crash_recovery_indexes_consistent`
    - `crash_recovery_repeated_cycles`
  - Validation: focused crash suite + full engine suite passing

- [x] **4. Auth + TLS end-to-end validation**
  - Verify unauthenticated requests rejected (401)
  - Verify overlay masking through HTTP wire path
  - Verify TLS with real certificate
  - Automated:
    - `unauthenticated_request_rejected` (HTTP query returns 401)
    - `overlay_strips_excluded_fields_through_http` (redacted vs full overlay through HTTP)
  - Manual:
    - HTTPS health succeeded with self-signed cert (`curl -sk https://.../api/health`)
    - Plain HTTP failed when TLS enabled (`http_code 000`)

- [x] **5. Cold-start p95 spike for latest_50**
  - Implemented startup pre-warm before bind in `dnadb_http`
  - Runtime now discovers known collections and pre-probes each configured sort index
  - Validation (cold restart, `ITERS=30`): `latest_50 avg=1.906ms`, `p95=1.782ms`, `min=0.370ms`
  - Note: a single first-call outlier still appears in `max` (~40ms), but p95 spike is eliminated

## Should-Haves (Ship Soon After)

- [x] **6. 1M CMS query validation**
  - Validation run completed at 1,000,000 rows (cold start, `ITERS=30`).
  - Prefix regression fixed (`LIKE 'post-9%' ORDER BY id LIMIT 50` now routes through id-sort scan path).
  - Final 1M matrix:
    - `latest_50`: avg 1.813ms, p95 1.649ms
    - `published_ordered`: avg 0.531ms, p95 0.655ms
    - `published_limit_only`: avg 0.439ms, p95 0.519ms
    - `point_id`: avg 0.209ms, p95 0.285ms
    - `exact_slug`: avg 0.189ms, p95 0.225ms
    - `prefix_slug_like`: avg 1.535ms, p95 1.662ms
    - `contains_slug_scan`: avg 1.119ms, p95 1.303ms

- [x] **7. Configurable sort indexes**
  - Per-collection sort/composite/exact-string index config persisted in `<collection>.sort_indexes.json`
  - Engine + HTTP APIs already support configure/add/get/status flows
  - SDK surface now includes collection-level configuration methods (TypeScript + Python transport contracts)
  - Python SDK tests cover configure delegation (`test_collection_configure_routes_to_transport`)

- [-] **8. Compaction (durable WAL only)**
  - Raw journal and segment-merge compaction were **removed** with the raw ingest path.
  - Next step: **durable WAL rotation** — seal/archive WAL files at a size threshold, start a new WAL, delete archived files only after confirmed replay (see `DNADB_SPEC_V2.md`).
  - Prior raw-journal compaction / `SIGKILL` issues are obsolete; durable path uses `commit_inner` fsync ordering.

- [x] **9. Observability hooks**
  - Added `tracing` + `tracing-subscriber` structured request/server logs in `dnadb_http`
  - Added Prometheus-style metrics endpoint: `GET /api/metrics`
  - Added slow query warnings with configurable threshold (`DNADB_SLOW_QUERY_MS`, default 25ms)
  - Exposed counters/gauges for requests, errors, overloads, inflight, latency buckets, slow queries

- [x] **10. Connection limits and backpressure**
  - Added in-flight request cap via semaphore middleware (`--max-connections`, env `DNADB_HTTP_MAX_CONNECTIONS`)
  - Over-cap requests rejected gracefully with HTTP 503 JSON response
  - Overload rejections and request timings exported in `/api/metrics`

- [ ] **11. SDK persistent HTTP transport (pool + keep-alive)**
  - Add first-class HTTP transport for TypeScript + Python SDKs with connection reuse
  - Requirements:
    - Keep-alive pooled clients by default (no per-request TCP/TLS handshake)
    - Bounded connection pool + timeouts + retry/backoff policy for transient failures
    - Graceful shutdown/close hooks and basic transport metrics
  - Validate with sustained write/read microbench against `dnadb_http`
  - Estimate: 1-2 days

## Nice-to-Have Before Wide Release

- [ ] **12. Admin API unification**
  - Consolidate rebuild/compact/stats/config into stable admin surface

- [ ] **13. Docker image + env documentation**
  - Publish image + production env variable guide

- [ ] **14. SDK polish**
  - Publish SDKs, retries/backoff, pooling, typed errors, docs

## Execution Order

1. Prefix superlinearity fix
2. Canonical materializer runtime path
3. Crash recovery tests
4. Auth/TLS e2e validation
5. Cold-start p95 prewarm
6. 1M CMS validation
7. Remaining post-RC items
