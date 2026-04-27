# Benchmark datasets (local)

This directory is for **large or sensitive corpora** you do not want in git. Keep it gitignored or use sparse checkouts.

## Layout

- Optional subfolders per scenario, e.g. `bench-data/ingest-500k/`, `bench-data/query-heavy/`.
- The engine does not require a fixed layout; paths are passed to `load_bench` or your own harness.

## Speed check workflow

1. **Build release:** `cd engine && cargo build --release --bin load_bench`
2. **Ingest phase** (adjust paths and flags to match your collection):

   ```bash
   ./target/release/load_bench \
     --data-dir /path/to/bench-data/run1 \
     --phase ingest \
     ... 
   ```

3. **Query / verify phases:** use `--phase verify`, `--phase query`, or full gate scripts documented in the repo root `README.md`.
4. **Compare runs:** save JSON output per run; diff fields such as `writes/sec`, `query_*_seconds`, `*_skipped_blocks`, and WAL/storage timings.

## After you add data

Point `load_bench` (or integration tests) at the directory you created and record baseline numbers once; subsequent changes should use the same flags for apples-to-apples comparison.
