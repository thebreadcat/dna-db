#!/usr/bin/env python3
"""Production-reality gate for DNA-DB.

Maps the user's 6 "real DB" requirements to executable checks.
"""

from __future__ import annotations

import argparse
import json
import selectors
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class GateCheck:
    gate: str
    name: str
    cwd: Path
    command: str
    heavy: bool = False


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def run_command(cwd: Path, command: str, check_name: str) -> tuple[bool, str]:
    proc = subprocess.Popen(
        command,
        cwd=cwd,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
    )
    assert proc.stdout is not None
    selector = selectors.DefaultSelector()
    selector.register(proc.stdout, selectors.EVENT_READ)

    started = time.time()
    last_heartbeat = started
    output_lines: list[str] = []
    heartbeat_interval_s = 15.0

    while True:
        events = selector.select(timeout=1.0)
        if events:
            line = proc.stdout.readline()
            if line:
                clean = line.rstrip("\n")
                output_lines.append(clean)
                print(f"    {clean}")
            elif proc.poll() is not None:
                break
        else:
            now = time.time()
            if proc.poll() is not None:
                break
            if now - last_heartbeat >= heartbeat_interval_s:
                elapsed_s = int(now - started)
                print(f"    .. {check_name} running ({elapsed_s}s elapsed)")
                last_heartbeat = now

    selector.unregister(proc.stdout)
    proc.wait()
    output = "\n".join(output_lines).strip()
    return proc.returncode == 0, output


def parse_trailing_json_object(output: str) -> dict[str, Any] | None:
    """Best-effort parse of trailing JSON payload from command output."""
    if not output:
        return None
    for idx in range(len(output) - 1, -1, -1):
        if output[idx] != "{":
            continue
        candidate = output[idx:].strip()
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def top_ingest_costs(report: dict[str, Any], top_n: int = 3) -> list[tuple[str, float]]:
    pairs = [
        ("storage_sync", float(report.get("ingest_storage_sync_seconds", 0.0))),
        ("wal_sync", float(report.get("ingest_wal_sync_seconds", 0.0))),
        ("wal_append", float(report.get("ingest_wal_append_seconds", 0.0))),
        ("storage_append", float(report.get("ingest_storage_append_seconds", 0.0))),
        ("prep", float(report.get("ingest_prep_seconds", 0.0))),
        ("encode", float(report.get("ingest_encode_seconds", 0.0))),
    ]
    pairs.sort(key=lambda p: p[1], reverse=True)
    return [p for p in pairs if p[1] > 0][:top_n]


def pct(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return (part / total) * 100.0


def clamp01(v: float) -> float:
    return max(0.0, min(1.0, v))


def skip_effectiveness_score(report: dict[str, Any]) -> tuple[float, str]:
    """Heuristic 0-100 score for skip-layer effectiveness.

    Higher score means:
    - query/negative query skip rates are high (especially block-level),
    - full scan skip rates are low (we don't want over-pruning false positives there),
    - query path is materially faster than full scan path.
    """
    segment_count = int(report.get("segment_count", 0))
    block_count = int(report.get("block_count", 0))
    q_skip_seg = pct(int(report.get("query_skipped_segments", 0)), segment_count) / 100.0
    q_skip_blk = pct(int(report.get("query_skipped_blocks", 0)), block_count) / 100.0
    neg_skip_seg = pct(int(report.get("negative_exact_skipped_segments", 0)), segment_count) / 100.0
    neg_skip_blk = pct(int(report.get("negative_exact_skipped_blocks", 0)), block_count) / 100.0
    fs_skip_seg = pct(int(report.get("full_scan_skipped_segments", 0)), segment_count) / 100.0
    fs_skip_blk = pct(int(report.get("full_scan_skipped_blocks", 0)), block_count) / 100.0

    query_s = float(report.get("query_seconds", 0.0))
    full_scan_s = float(report.get("full_scan_seconds", 0.0))
    # Positive when query is faster than full scan.
    speed_gain = clamp01((full_scan_s - query_s) / max(full_scan_s, 1e-9))

    # Weighted for selective-query behavior first, then negative probe behavior.
    # Penalize high full-scan skipping (usually indicates little/no selectivity signal).
    score01 = (
        0.30 * q_skip_blk
        + 0.20 * q_skip_seg
        + 0.20 * neg_skip_blk
        + 0.10 * neg_skip_seg
        + 0.15 * speed_gain
        + 0.05 * (1.0 - fs_skip_blk)
    ) - (0.10 * fs_skip_seg)
    score01 = clamp01(score01)
    score = score01 * 100.0
    rationale = (
        f"q_blk={q_skip_blk*100:.1f}% q_seg={q_skip_seg*100:.1f}% "
        f"neg_blk={neg_skip_blk*100:.1f}% speed_gain={speed_gain*100:.1f}% "
        f"fs_blk={fs_skip_blk*100:.1f}%"
    )
    return score, rationale


def print_benchmark_bottlenecks(results: list[dict[str, Any]]) -> None:
    bench_rows = [
        r for r in results if isinstance(r.get("benchmark_report"), dict) and r["name"].startswith("load_bench_")
    ]
    if not bench_rows:
        return
    print("\nBottleneck summary (load_bench):")
    for row in bench_rows:
        rep = row["benchmark_report"]
        phase = rep.get("phase", "unknown")
        write_s = float(rep.get("write_seconds", 0.0))
        decode_s = float(rep.get("decode_seconds", 0.0))
        query_s = float(rep.get("query_seconds", 0.0))
        full_scan_s = float(rep.get("full_scan_seconds", 0.0))
        wps = float(rep.get("write_records_per_sec", 0.0))
        grows = int(rep.get("storage_total_grow_events", 0))
        fsyncs = int(rep.get("ingest_fsync_events", rep.get("sync_events", 0)))
        segment_count = int(rep.get("segment_count", 0))
        block_count = int(rep.get("block_count", 0))
        seg_dict_entries = int(rep.get("segment_dictionary_entries", 0))
        blk_dict_entries = int(rep.get("block_dictionary_entries", 0))
        seg_dict_cov = float(rep.get("segment_dictionary_coverage_pct", 0.0))
        blk_dict_cov = float(rep.get("block_dictionary_coverage_pct", 0.0))
        q_skip_seg = int(rep.get("query_skipped_segments", 0))
        q_skip_blk = int(rep.get("query_skipped_blocks", 0))
        q_skip_seg_dict_extra = int(rep.get("query_skipped_segments_dict_extra", 0))
        q_skip_blk_dict_extra = int(rep.get("query_skipped_blocks_dict_extra", 0))
        fs_skip_seg = int(rep.get("full_scan_skipped_segments", 0))
        fs_skip_blk = int(rep.get("full_scan_skipped_blocks", 0))
        fs_skip_seg_dict_extra = int(rep.get("full_scan_skipped_segments_dict_extra", 0))
        fs_skip_blk_dict_extra = int(rep.get("full_scan_skipped_blocks_dict_extra", 0))
        neg_skip_seg = int(rep.get("negative_exact_skipped_segments", 0))
        neg_skip_blk = int(rep.get("negative_exact_skipped_blocks", 0))
        neg_skip_seg_dict_extra = int(rep.get("negative_exact_skipped_segments_dict_extra", 0))
        neg_skip_blk_dict_extra = int(rep.get("negative_exact_skipped_blocks_dict_extra", 0))
        score, rationale = skip_effectiveness_score(rep)
        top_costs = ", ".join(f"{name}={seconds:.2f}s" for name, seconds in top_ingest_costs(rep))
        if not top_costs:
            top_costs = "n/a"
        print(
            f"- {row['name']} ({phase}): "
            f"write={write_s:.2f}s decode={decode_s:.2f}s query={query_s:.3f}s full_scan={full_scan_s:.3f}s "
            f"writes/s={wps:.0f} fsync_events={fsyncs} remaps={grows} "
            f"segments={segment_count} blocks={block_count} "
            f"skip(query)={q_skip_seg}/{segment_count} ({pct(q_skip_seg, segment_count):.1f}%) "
            f"skip_blocks(query)={q_skip_blk}/{block_count} ({pct(q_skip_blk, block_count):.1f}%) "
            f"dict_extra(query)=seg:{q_skip_seg_dict_extra} blk:{q_skip_blk_dict_extra} "
            f"dict(seg)={seg_dict_entries} cov={seg_dict_cov:.1f}% "
            f"dict(block)={blk_dict_entries} cov={blk_dict_cov:.1f}% "
            f"skip(full)={fs_skip_seg}/{segment_count} ({pct(fs_skip_seg, segment_count):.1f}%) "
            f"skip_blocks(full)={fs_skip_blk}/{block_count} ({pct(fs_skip_blk, block_count):.1f}%) "
            f"dict_extra(full)=seg:{fs_skip_seg_dict_extra} blk:{fs_skip_blk_dict_extra} "
            f"skip(neg)={neg_skip_seg}/{segment_count} ({pct(neg_skip_seg, segment_count):.1f}%) "
            f"skip_blocks(neg)={neg_skip_blk}/{block_count} ({pct(neg_skip_blk, block_count):.1f}%) "
            f"dict_extra(neg)=seg:{neg_skip_seg_dict_extra} blk:{neg_skip_blk_dict_extra} "
            f"skip_score={score:.1f}/100 ({rationale}) "
            f"top_ingest=[{top_costs}]"
        )


def default_checks(root: Path, include_heavy: bool, include_10m_growth: bool) -> list[GateCheck]:
    bench_root_100k = Path(tempfile.mkdtemp(prefix="dnadb-real-gate-100k-"))
    checks = [
        # 1) Writes are durable: crash replay and recovery.
        GateCheck(
            gate="1_durability",
            name="crash_recovery_end_to_end",
            cwd=root / "engine",
            command="cargo test -q crash_recovery_end_to_end",
        ),
        GateCheck(
            gate="1_durability",
            name="replication_resume_checkpoint",
            cwd=root / "engine",
            command="cargo test -q replication_stream_resumes_from_checkpoint",
        ),
        # 2) Reads correct under concurrency: MVCC snapshot isolation + conflicts.
        GateCheck(
            gate="2_concurrency_correctness",
            name="mvcc_snapshot_isolation",
            cwd=root / "engine",
            command="cargo test -q mvcc_snapshot_isolation_end_to_end",
        ),
        GateCheck(
            gate="2_concurrency_correctness",
            name="write_write_conflict_rejection",
            cwd=root / "engine",
            command="cargo test -q conflicting_writes_reject_later_commit",
        ),
        # 3) Index consistency after crash/update/reload.
        GateCheck(
            gate="3_index_consistency",
            name="hash_index_roundtrip",
            cwd=root / "engine",
            command="cargo test -q global_hash_index_persists_and_loads",
        ),
        GateCheck(
            gate="3_index_consistency",
            name="catalog_roundtrip",
            cwd=root / "engine",
            command="cargo test -q catalog_roundtrip_save_and_load",
        ),
        # 4) Compaction over time with no loss.
        GateCheck(
            gate="4_compaction_health",
            name="histone_compaction_clustering",
            cwd=root / "engine",
            command="cargo test -q compact_segments_clusters_by_foreign_key",
        ),
        GateCheck(
            gate="4_compaction_health",
            name="mvcc_compaction_snapshot_safety",
            cwd=root / "engine",
            command="cargo test -q compaction_preserves_oldest_active_snapshot_visibility",
        ),
        # 5) Planner chooses paths based on data.
        GateCheck(
            gate="5_planner_adaptivity",
            name="planner_multi_candidate_selectivity",
            cwd=root / "engine",
            command="cargo test -q planner_picks_most_selective_index_from_multiple_candidates",
        ),
        GateCheck(
            gate="5_planner_adaptivity",
            name="planner_nonselective_fallback",
            cwd=root / "engine",
            command="cargo test -q planner_prefers_guided_scan_when_index_is_non_selective",
        ),
        # 6) Performance stability (1k/1m baseline).
        GateCheck(
            gate="6_performance_stability",
            name="spec_compliance_100k",
            cwd=root,
            command=f"python3 scripts/test_spec_compliance.py --records 100000 --data-dir {bench_root_100k}",
        ),
    ]

    if include_heavy:
        bench_root_1m = Path(tempfile.mkdtemp(prefix="dnadb-real-gate-1m-"))
        bench_root_100m = Path(tempfile.mkdtemp(prefix="dnadb-real-gate-100m-"))
        checks.extend(
            [
                GateCheck(
                    gate="6_performance_stability",
                    name="load_bench_1m_balanced",
                    cwd=root / "engine",
                    command=(
                        "cargo run --release --bin load_bench -- "
                        "--mode balanced --records 1000000 --read-sample 10000 "
                        "--batch-size 1000 --threads 16 --prep-batch 10000 "
                        "--wal-interval-ms 0 --mmap-bytes 2634217728 "
                        f"--data-dir {bench_root_1m}"
                    ),
                    heavy=True,
                ),
                GateCheck(
                    gate="6_performance_stability",
                    name="load_bench_100m_ingest",
                    cwd=root / "engine",
                    command=(
                        "cargo run --release --bin load_bench -- "
                        "--mode balanced --records 100000000 --read-sample 10000 "
                        "--batch-size 1000 --threads 16 --prep-batch 20000 "
                        "--wal-interval-ms 0 --mmap-bytes 32212254720 --phase ingest "
                        f"--data-dir {bench_root_100m}"
                    ),
                    heavy=True,
                ),
                GateCheck(
                    gate="6_performance_stability",
                    name="load_bench_100m_verify",
                    cwd=root / "engine",
                    command=(
                        "cargo run --release --bin load_bench -- "
                        "--mode balanced --records 100000000 --read-sample 10000 "
                        "--batch-size 1000 --threads 16 --prep-batch 20000 "
                        "--wal-interval-ms 0 --mmap-bytes 32212254720 --phase verify "
                        f"--data-dir {bench_root_100m}"
                    ),
                    heavy=True,
                ),
            ]
        )
    if include_10m_growth:
        bench_root_10m = Path(tempfile.mkdtemp(prefix="dnadb-real-gate-10m-"))
        checks.append(
            GateCheck(
                gate="6_performance_stability",
                name="growth_10m_balanced",
                cwd=root / "engine",
                command=(
                    "cargo run --release --bin load_bench -- "
                    "--mode balanced --records 10000000 --read-sample 10000 "
                    "--batch-size 1000 --threads 16 --prep-batch 10000 "
                    "--wal-interval-ms 0 --mmap-bytes 12884901888 "
                    f"--data-dir {bench_root_10m}"
                ),
                heavy=True,
            )
        )

    return checks


def summarize(results: list[dict[str, Any]]) -> dict[str, Any]:
    by_gate: dict[str, dict[str, Any]] = {}
    for r in results:
        gate = r["gate"]
        g = by_gate.setdefault(gate, {"total": 0, "passed": 0, "checks": []})
        g["total"] += 1
        if r["passed"]:
            g["passed"] += 1
        g["checks"].append(r)
    gates = {}
    for gate, data in by_gate.items():
        gates[gate] = {
            "passed": data["passed"] == data["total"],
            "passed_checks": data["passed"],
            "total_checks": data["total"],
        }
    overall_passed = all(v["passed"] for v in gates.values()) if gates else False
    return {
        "overall_passed": overall_passed,
        "gate_summary": gates,
        "checks": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run real database acceptance gate")
    parser.add_argument(
        "--include-heavy",
        action="store_true",
        help="Include heavy perf checks (1M + 100M record runs).",
    )
    parser.add_argument(
        "--include-growth-10m",
        action="store_true",
        help="Include explicit 10M growth test run.",
    )
    parser.add_argument(
        "--soak-hours",
        type=float,
        default=0.0,
        help="Optional soak run duration in hours (repeats gate checks in a loop).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON summary only.",
    )
    args = parser.parse_args()

    root = repo_root()
    all_results: list[dict[str, Any]] = []
    iteration = 0
    start_ts = __import__("time").time()
    while True:
        iteration += 1
        checks = default_checks(
            root,
            include_heavy=args.include_heavy,
            include_10m_growth=args.include_growth_10m,
        )
        print(f"\n=== Gate iteration {iteration} ===")
        iteration_results: list[dict[str, Any]] = []
        for idx, check in enumerate(checks, start=1):
            print(f"[{idx}/{len(checks)}] {check.gate}::{check.name}")
            ok, output = run_command(check.cwd, check.command, check.name)
            benchmark_report = (
                parse_trailing_json_object(output)
                if "load_bench" in check.command
                else None
            )
            iteration_results.append(
                {
                    "iteration": iteration,
                    "gate": check.gate,
                    "name": check.name,
                    "command": check.command,
                    "passed": ok,
                    "heavy": check.heavy,
                    "benchmark_report": benchmark_report,
                    "output_tail": "\n".join(output.splitlines()[-20:]) if output else "",
                }
            )
            print("  -> PASS" if ok else "  -> FAIL")
        all_results.extend(iteration_results)
        summary = summarize(iteration_results)
        if not summary["overall_passed"]:
            break
        if args.soak_hours <= 0:
            break
        elapsed_h = (__import__("time").time() - start_ts) / 3600.0
        if elapsed_h >= args.soak_hours:
            break

    summary = summarize(all_results)
    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        print("\nGate summary:")
        for gate, meta in summary["gate_summary"].items():
            status = "PASS" if meta["passed"] else "FAIL"
            print(f"- {gate}: {status} ({meta['passed_checks']}/{meta['total_checks']})")
        print_benchmark_bottlenecks(all_results)

    if not summary["overall_passed"]:
        if not args.json:
            print("\nFailed checks:")
            for r in all_results:
                if not r["passed"]:
                    print(f"- {r['gate']}::{r['name']}")
                    if r["output_tail"]:
                        print(r["output_tail"])
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

