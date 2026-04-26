#!/usr/bin/env python3
"""Run benchmark matrix and assert against v2 spec performance thresholds."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any


SPEC_TARGETS = {
    "strict_writes_per_sec": 2000.0,
    "balanced_writes_per_sec": 5000.0,
    "full_scan_100k_ms": 5.0,
    "single_query_ms": 5.0,
    "decode_per_sec": 100_000.0,
}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def run_bench_matrix(root: Path, records: int, data_dir: Path | None) -> dict[str, Any]:
    cmd = [
        "python3",
        "scripts/bench_matrix.py",
        "--records",
        str(records),
        "--read-sample",
        "10000",
        "--threads",
        "16",
        "--prep-batch",
        "10000",
        "--wal-interval-ms",
        "0",
    ]
    if data_dir is not None:
        cmd.extend(["--data-dir", str(data_dir)])
    proc = subprocess.run(
        cmd,
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"bench_matrix failed:\n{proc.stdout}\n{proc.stderr}")

    # bench_matrix prints JSON first, then a markdown table. Parse first JSON object.
    decoder = json.JSONDecoder()
    out = proc.stdout.lstrip()
    payload, _ = decoder.raw_decode(out)
    return payload


def metric_snapshot(payload: dict[str, Any]) -> dict[str, float]:
    by_mode = {r["mode"]: r for r in payload["reports"]}
    strict = by_mode["strict"]
    balanced = by_mode["balanced"]
    fast = by_mode["fast"]
    return {
        "strict_writes_per_sec": float(strict["write_records_per_sec"]),
        "balanced_writes_per_sec": float(balanced["write_records_per_sec"]),
        "full_scan_100k_ms": float(fast["full_scan_seconds"]) * 1000.0,
        "single_query_ms": float(fast["query_seconds"]) * 1000.0,
        "decode_per_sec": float(fast["decode_records_per_sec"]),
    }


def assert_targets(metrics: dict[str, float]) -> list[str]:
    failures: list[str] = []
    for key, target in SPEC_TARGETS.items():
        actual = metrics[key]
        if key.endswith("_ms"):
            if actual > target:
                failures.append(f"{key}: {actual:.3f}ms exceeds spec {target:.3f}ms")
        else:
            if actual < target:
                failures.append(f"{key}: {actual:.1f} below spec {target:.1f}")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description="Assert benchmark metrics against spec thresholds")
    parser.add_argument("--records", type=int, default=100000)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("/tmp/dnadb-spec-compliance"),
        help="Parent data dir for matrix runs",
    )
    args = parser.parse_args()

    root = repo_root()
    payload = run_bench_matrix(root, records=args.records, data_dir=args.data_dir)
    metrics = metric_snapshot(payload)
    failures = assert_targets(metrics)

    print(json.dumps({"targets": SPEC_TARGETS, "metrics": metrics}, indent=2))
    if failures:
        print("\nSpec compliance failed:")
        for line in failures:
            print(f"- {line}")
        return 1
    print("\nAll spec targets met.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

