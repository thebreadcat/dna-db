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
  --mmap-bytes "${MMAP_BYTES}"
  --data-dir /tmp/dnadb-bench-data
)
if [[ -n "${WAL_INTERVAL_MS}" ]]; then
  LOAD_BENCH_ARGS+=(--wal-interval-ms "${WAL_INTERVAL_MS}")
fi

docker run --rm \
  -e RECORDS="${RECORDS}" \
  -e READ_SAMPLE="${READ_SAMPLE}" \
  -e MMAP_BYTES="${MMAP_BYTES}" \
  dnadb-load-bench \
  "${LOAD_BENCH_ARGS[@]}" | tee "${OUT_FILE}"

echo
echo "Benchmark report saved to ${OUT_FILE}"
