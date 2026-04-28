#!/usr/bin/env python3
"""DNA-DB admin/inspection baseline CLI (Stage 8)."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _read_progress(root: Path) -> str | None:
    """Return progress.md text, or None if absent (e.g. public CI clone — file is gitignored)."""
    path = root / "progress.md"
    if not path.is_file():
        return None
    return path.read_text(encoding="utf-8")


def inspect_stage_status(root: Path) -> dict[str, Any]:
    text = _read_progress(root)
    if text is None:
        return {
            "completed_stages": 0,
            "total_stages": 0,
            "stages": [],
            "progress_md": "missing",
        }
    stage_lines = re.findall(r"^- \[(x| )\] (Stage [0-9]+ - .+)$", text, re.MULTILINE)
    stages: list[dict[str, Any]] = []
    for mark, title in stage_lines:
        stages.append({"stage": title, "complete": mark == "x"})
    completed = sum(1 for s in stages if s["complete"])
    return {
        "completed_stages": completed,
        "total_stages": len(stages),
        "stages": stages,
        "progress_md": "present",
    }


def inspect_build_completion(root: Path) -> dict[str, Any]:
    text = _read_progress(root)
    if text is None:
        return {"rows": [], "progress_md": "missing"}
    row_matches = re.findall(
        r"^\| \*\*(.+?)\*\* .*?\| \*\*(.+?)\*\* \| \*\*(.+?)\*\* \|$",
        text,
        re.MULTILINE,
    )
    rows = [
        {"scope": scope.strip(), "complete": complete.strip(), "remaining": remaining.strip()}
        for scope, complete, remaining in row_matches
    ]
    return {"rows": rows, "progress_md": "present"}


def inspect_sdk_status(root: Path) -> dict[str, Any]:
    ts_index = root / "sdk" / "typescript" / "src" / "index.ts"
    py_client = root / "sdk" / "python" / "dnadb" / "client.py"
    return {
        "typescript_sdk_present": ts_index.exists(),
        "python_sdk_present": py_client.exists(),
        "typescript_entry": str(ts_index.relative_to(root)),
        "python_entry": str(py_client.relative_to(root)),
    }


def inspect_all(root: Path) -> dict[str, Any]:
    return {
        "stage_status": inspect_stage_status(root),
        "build_completion": inspect_build_completion(root),
        "sdk_status": inspect_sdk_status(root),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DNA-DB admin/inspection baseline CLI")
    parser.add_argument(
        "command",
        choices=["status", "completion", "sdk", "all"],
        help="Inspection command",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON output",
    )
    return parser


def _render_text(command: str, payload: dict[str, Any]) -> str:
    if command == "status":
        lines = [
            f"Stages complete: {payload['completed_stages']}/{payload['total_stages']}",
        ]
        if payload.get("progress_md") == "missing":
            lines.append("(note: progress.md not in repo — stage list empty)")
        for row in payload["stages"]:
            prefix = "[x]" if row["complete"] else "[ ]"
            lines.append(f"{prefix} {row['stage']}")
        return "\n".join(lines)
    if command == "completion":
        lines = ["Build completion rows:"]
        if payload.get("progress_md") == "missing":
            lines.append("(note: progress.md not in repo — no rows)")
        for row in payload["rows"]:
            lines.append(
                f"- {row['scope']}: complete={row['complete']} remaining={row['remaining']}"
            )
        return "\n".join(lines)
    if command == "sdk":
        return (
            "SDK status:\n"
            f"- TypeScript present: {payload['typescript_sdk_present']} ({payload['typescript_entry']})\n"
            f"- Python present: {payload['python_sdk_present']} ({payload['python_entry']})"
        )
    return json.dumps(payload, indent=2)


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    root = _repo_root()

    if args.command == "status":
        payload = inspect_stage_status(root)
    elif args.command == "completion":
        payload = inspect_build_completion(root)
    elif args.command == "sdk":
        payload = inspect_sdk_status(root)
    else:
        payload = inspect_all(root)

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(_render_text(args.command, payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
