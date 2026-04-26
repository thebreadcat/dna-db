#!/usr/bin/env python3
"""Run load benchmark across durability modes and print comparison table."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ModeRun:
    mode: str
    batch_size: int


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def run_mode(
    root: Path,
    records: int,
    read_sample: int,
    mmap_bytes: int,
    threads: int,
    prep_batch: int,
    wal_interval_ms: int | None,
    concurrent_writers: bool,
    matrix_base: Path,
    run: ModeRun,
) -> dict[str, Any]:
    # Fresh dir per matrix session avoids WAL/strand accumulation when re-running.
    data_dir = matrix_base / run.mode
    data_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "cargo",
        "run",
        "--release",
        "--bin",
        "load_bench",
        "--",
        "--records",
        str(records),
        "--read-sample",
        str(read_sample),
        "--mmap-bytes",
        str(mmap_bytes),
        "--mode",
        run.mode,
        "--batch-size",
        str(run.batch_size),
        "--threads",
        str(threads),
        "--prep-batch",
        str(prep_batch),
        "--data-dir",
        str(data_dir),
    ]
    if wal_interval_ms is not None:
        cmd.extend(["--wal-interval-ms", str(wal_interval_ms)])
    if concurrent_writers:
        cmd.append("--concurrent-writers")
    proc = subprocess.run(
        cmd,
        cwd=root / "engine",
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"{run.mode} failed:\n{proc.stdout}\n{proc.stderr}")
    # JSON is printed as final stdout object.
    out = proc.stdout.strip()
    json_start = out.find("{")
    if json_start == -1:
        raise RuntimeError(f"no JSON in output for mode {run.mode}")
    return json.loads(out[json_start:])


def main() -> int:
    parser = argparse.ArgumentParser(description="Run strict/balanced/fast benchmark matrix")
    parser.add_argument("--records", type=int, default=100000)
    parser.add_argument("--read-sample", type=int, default=10000)
    parser.add_argument("--mmap-bytes", type=int, default=536870912)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--prep-batch", type=int, default=5000)
    parser.add_argument(
        "--wal-interval-ms",
        type=int,
        default=None,
        help="Override load_bench WAL timer for strict/balanced; omitted uses mode defaults",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Optional parent dir for matrix datasets (default: <repo>/bench-data-matrix-<timestamp>)",
    )
    parser.add_argument(
        "--concurrent-writers",
        action="store_true",
        help="Enable true concurrent write threads in load_bench",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Write JSON + markdown table under bench-output/",
    )
    args = parser.parse_args()

    root = repo_root()
    session = time.strftime("%Y%m%d-%H%M%S")
    matrix_base = args.data_dir if args.data_dir is not None else root / f"bench-data-matrix-{session}"
    matrix_base = matrix_base.resolve()
    matrix_base.mkdir(parents=True, exist_ok=True)
    # strict/balanced: group-commit batch (WAL+storage fsync cadence). fast: mid-loop fsync off.
    modes = [
        ModeRun("strict", 1000),
        ModeRun("balanced", 1000),
        ModeRun("fast", 5000),
    ]
    reports = [
        run_mode(
            root,
            args.records,
            args.read_sample,
            args.mmap_bytes,
            args.threads,
            args.prep_batch,
            args.wal_interval_ms,
            args.concurrent_writers,
            matrix_base,
            m,
        )
        for m in modes
    ]

    payload = {
        "session": session,
        "matrix_data_dir": str(matrix_base),
        "threads": args.threads,
        "prep_batch": args.prep_batch,
        "wal_interval_ms": args.wal_interval_ms,
        "concurrent_writers": args.concurrent_writers,
        "reports": reports,
    }
    print(json.dumps(payload, indent=2))
    print()
    print("| mode | write_s | writes/s | query_ms | full_scan_ms |")
    print("|---|---:|---:|---:|---:|")
    lines = [
        "| mode | write_s | writes/s | query_ms | full_scan_ms |",
        "|---|---:|---:|---:|---:|",
    ]
    for r in reports:
        row = (
            f"| {r['mode']} | {r['write_seconds']:.3f} | {r['write_records_per_sec']:.1f} | "
            f"{r['query_seconds']*1000:.3f} | {r['full_scan_seconds']*1000:.3f} |"
        )
        print(row)
        lines.append(row)

    if args.save:
        out_dir = root / "bench-output"
        out_dir.mkdir(parents=True, exist_ok=True)
        base = out_dir / f"bench-matrix-{session}"
        base.with_suffix(".json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        base.with_suffix(".md").write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"\nSaved {base}.json and {base}.md", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
