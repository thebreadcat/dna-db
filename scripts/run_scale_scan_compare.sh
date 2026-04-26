#!/usr/bin/env bash
# Run load_bench at 100k and 1M on local disk (default /tmp) for write + scan scaling comparison.
# Usage: ./scripts/run_scale_scan_compare.sh [data_root]
set -euo pipefail
ROOT="${1:-/tmp}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${MODE:-fast}"
BATCH="${BATCH_SIZE:-5000}"

# Default: ~2.5 KiB headroom per record + 128 MiB; cap 8 GiB (override with MMAP_BYTES).
mmap_for_records() {
  local n="$1"
  local m=$(( n * 2500 + 134217728 ))
  local cap=8589934592
  if [[ -n "${MMAP_BYTES:-}" ]]; then
    echo "${MMAP_BYTES}"
  elif (( m > cap )); then
    echo "${cap}"
  else
    echo "${m}"
  fi
}

run() {
  local n="$1" dir="$2" out="$3"
  local mmap
  mmap="$(mmap_for_records "${n}")"
  echo "=== ${n} records → ${dir} (mmap-bytes=${mmap}) ==="
  (cd "${REPO}/engine" && cargo run --release --bin load_bench -- \
    --mode "${MODE}" \
    --records "${n}" \
    --read-sample 10000 \
    --batch-size "${BATCH}" \
    --mmap-bytes "${mmap}" \
    --data-dir "${dir}") | tee "${out}"
}

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="${REPO}/bench-output"
mkdir -p "${OUT}"

run 100000 "${ROOT}/dnadb-scale-100k-${STAMP}" "${OUT}/scale-100k-${STAMP}.json"
run 1000000 "${ROOT}/dnadb-scale-1m-${STAMP}" "${OUT}/scale-1m-${STAMP}.json"

echo
echo "Saved under ${OUT}/scale-*-${STAMP}.json"
echo "Compare: query_s_per_million_strands, full_scan_s_per_million_strands (should stay ~flat if per-strand work dominates)."
