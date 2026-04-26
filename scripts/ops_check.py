#!/usr/bin/env python3
"""Deployment and monitoring baseline checks for DNA-DB."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def check_required_files(root: Path) -> dict[str, Any]:
    required = [
        root / "docker-compose.observability.yml",
        root / "config" / "dnadb.config.toml.example",
        root / "docs" / "DEPLOYMENT_MONITORING.md",
    ]
    files = [
        {"path": str(path.relative_to(root)), "present": path.exists()} for path in required
    ]
    ok = all(item["present"] for item in files)
    return {"ok": ok, "files": files}


def check_http_endpoint(url: str, timeout: float) -> dict[str, Any]:
    try:
        with urlopen(url, timeout=timeout) as response:
            status = response.status
            return {"url": url, "ok": 200 <= status < 400, "status": status}
    except URLError as exc:
        return {"url": url, "ok": False, "error": str(exc)}


def check_runtime_endpoints(timeout: float) -> dict[str, Any]:
    default_health = os.environ.get("DNADB_HEALTH_URL", "http://127.0.0.1:8080/healthz")
    default_metrics = os.environ.get("DNADB_METRICS_URL", "http://127.0.0.1:9090/-/healthy")
    checks = [
        check_http_endpoint(default_health, timeout),
        check_http_endpoint(default_metrics, timeout),
    ]
    ok = all(item.get("ok", False) for item in checks)
    return {"ok": ok, "checks": checks}


def run_checks(timeout: float, skip_runtime: bool) -> dict[str, Any]:
    root = repo_root()
    file_checks = check_required_files(root)
    runtime_checks = (
        {"ok": True, "checks": [], "skipped": True}
        if skip_runtime
        else check_runtime_endpoints(timeout)
    )
    return {
        "ok": file_checks["ok"] and runtime_checks["ok"],
        "file_checks": file_checks,
        "runtime_checks": runtime_checks,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="DNA-DB deployment/monitoring baseline checks")
    parser.add_argument("--timeout", type=float, default=1.5, help="HTTP timeout in seconds")
    parser.add_argument(
        "--skip-runtime",
        action="store_true",
        help="Skip live endpoint checks (file/package checks only)",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON result")
    args = parser.parse_args()

    result = run_checks(args.timeout, args.skip_runtime)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"Overall: {'OK' if result['ok'] else 'NOT OK'}")
        for item in result["file_checks"]["files"]:
            mark = "OK" if item["present"] else "MISSING"
            print(f"[{mark}] {item['path']}")
        for item in result["runtime_checks"]["checks"]:
            if item.get("ok"):
                print(f"[OK] {item['url']} status={item['status']}")
            else:
                print(f"[FAIL] {item['url']} {item.get('error', '')}".strip())
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
