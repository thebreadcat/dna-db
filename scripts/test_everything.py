#!/usr/bin/env python3
"""Run DNA-DB end-to-end verification checks from one command."""

from __future__ import annotations

import argparse
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Check:
    name: str
    cwd: Path
    command: str


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_checks(root: Path, quick: bool) -> list[Check]:
    checks = [
        Check("engine-tests", root / "engine", "cargo test"),
        Check("engine-benches-check", root / "engine", "cargo check --benches"),
        Check("typescript-sdk", root / "sdk" / "typescript", "npm run check && npm run build"),
        Check("python-sdk-tests", root / "sdk" / "python", 'python3 -m unittest discover -s tests -p "test_*.py"'),
        Check("repo-tooling-tests", root, 'python3 -m unittest discover -s tests -p "test_*.py"'),
        Check("admin-inspect-smoke", root, "python3 scripts/admin_inspect.py all --json"),
    ]
    if quick:
        checks.append(Check("ops-check-smoke", root, "python3 scripts/ops_check.py --skip-runtime --json"))
    else:
        checks.append(Check("ops-check-runtime", root, "python3 scripts/ops_check.py --json"))
    return checks


def run_check(check: Check) -> tuple[bool, str]:
    proc = subprocess.run(
        check.command,
        cwd=check.cwd,
        shell=True,
        text=True,
        capture_output=True,
    )
    output = (proc.stdout or "") + (proc.stderr or "")
    return proc.returncode == 0, output.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run complete DNA-DB verification suite")
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Skip live runtime endpoint checks (uses packaging-only ops check).",
    )
    args = parser.parse_args()

    root = repo_root()
    checks = default_checks(root, quick=args.quick)
    failures: list[tuple[Check, str]] = []

    print(f"Running {len(checks)} checks from {root}...")
    for idx, check in enumerate(checks, start=1):
        print(f"[{idx}/{len(checks)}] {check.name}: {check.command}")
        ok, output = run_check(check)
        if ok:
            print(f"  -> PASS ({check.name})")
        else:
            print(f"  -> FAIL ({check.name})")
            failures.append((check, output))

    if failures:
        print("\nVerification failed.\n")
        for check, output in failures:
            print(f"=== {check.name} ({check.cwd}) ===")
            print(output or "(no output)")
            print()
        return 1

    print("\nAll verification checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
