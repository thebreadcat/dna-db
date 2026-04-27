#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${REPO_ROOT}/bench-output"
mkdir -p "${OUT_DIR}"

RECORDS="${RECORDS:-100000}"
READ_SAMPLE="${READ_SAMPLE:-10000}"
MMAP_BYTES="${MMAP_BYTES:-536870912}"
MODE="${MODE:-strict}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
WAL_INTERVAL_MS="${WAL_INTERVAL_MS:-}"
THREADS="${THREADS:-1}"
PREP_BATCH="${PREP_BATCH:-5000}"
CONCURRENT_WRITERS="${CONCURRENT_WRITERS:-0}"
WAL_SHARDS="${WAL_SHARDS:-1}"
ASYNC_SYNC="${ASYNC_SYNC:-0}"

# Container limits for reproducible apples-to-apples runs.
DOCKER_PLATFORM="${DOCKER_PLATFORM:-linux/arm64}"
DOCKER_CPUS="${DOCKER_CPUS:-8}"
DOCKER_MEMORY="${DOCKER_MEMORY:-16g}"
DOCKER_PIDS_LIMIT="${DOCKER_PIDS_LIMIT:-4096}"
DOCKER_ULIMIT_NOFILE="${DOCKER_ULIMIT_NOFILE:-1048576:1048576}"

echo "Building benchmark container..."
docker build -f "${REPO_ROOT}/Dockerfile.bench" -t dnadb-load-bench "${REPO_ROOT}"

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT_FILE="${OUT_DIR}/load-bench-${STAMP}.json"

echo "Running benchmark (mode=${MODE}, records=${RECORDS}, read_sample=${READ_SAMPLE}, batch_size=${BATCH_SIZE}${WAL_INTERVAL_MS:+, wal_interval_ms=${WAL_INTERVAL_MS}})..."
LOAD_BENCH_ARGS=(
  ./engine/target/release/load_bench
  --records "${RECORDS}"
  --read-sample "${READ_SAMPLE}"
  --mode "${MODE}"
  --batch-size "${BATCH_SIZE}"
  --threads "${THREADS}"
  --prep-batch "${PREP_BATCH}"
  --wal-shards "${WAL_SHARDS}"
  --mmap-bytes "${MMAP_BYTES}"
  --data-dir /tmp/dnadb-bench-data
)
if [[ -n "${WAL_INTERVAL_MS}" ]]; then
  LOAD_BENCH_ARGS+=(--wal-interval-ms "${WAL_INTERVAL_MS}")
fi
if [[ "${CONCURRENT_WRITERS}" == "1" ]]; then
  LOAD_BENCH_ARGS+=(--concurrent-writers)
fi
if [[ "${ASYNC_SYNC}" == "1" ]]; then
  LOAD_BENCH_ARGS+=(--async-sync)
fi

docker run --rm \
  --platform "${DOCKER_PLATFORM}" \
  --cpus "${DOCKER_CPUS}" \
  --memory "${DOCKER_MEMORY}" \
  --pids-limit "${DOCKER_PIDS_LIMIT}" \
  --ulimit "nofile=${DOCKER_ULIMIT_NOFILE}" \
  -e RECORDS="${RECORDS}" \
  -e READ_SAMPLE="${READ_SAMPLE}" \
  -e MMAP_BYTES="${MMAP_BYTES}" \
  dnadb-load-bench \
  "${LOAD_BENCH_ARGS[@]}" | tee "${OUT_FILE}"

echo
echo "Benchmark report saved to ${OUT_FILE}"
