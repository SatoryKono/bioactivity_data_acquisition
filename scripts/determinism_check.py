#!/usr/bin/env python3
"""Determinism check: verify that pipeline runs produce identical logs.

This script runs activity_chembl and assay_chembl pipelines twice with --dry-run,
compares the structured logs (ignoring timestamps), and reports any differences.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parent.parent
AUDIT_RESULTS = ROOT / "audit_results"


def normalize_log_line(line: str) -> str:
    """Normalize a log line by removing timestamps and other non-deterministic fields."""
    # Remove timestamps (ISO-8601 format)
    line = re.sub(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})', '', line)
    # Remove run_id (UUID format)
    line = re.sub(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '<run_id>', line, flags=re.IGNORECASE)
    # Remove other potential non-deterministic fields (duration_ms might vary slightly)
    # But keep structure intact
    return line.strip()


def extract_structured_logs(stdout: str, stderr: str) -> list[dict[str, Any]]:
    """Extract structured JSON logs from stdout/stderr."""
    logs: list[dict[str, Any]] = []
    all_output = stdout + "\n" + stderr

    # Look for JSON log lines (structlog format)
    for line in all_output.split("\n"):
        line = line.strip()
        if not line:
            continue

        # Try to parse as JSON (structlog outputs JSON)
        try:
            # Some loggers might output "event": {...} format
            if line.startswith("{") and line.endswith("}"):
                log_entry = json.loads(line)
                # Remove non-deterministic fields
                log_entry.pop("timestamp", None)
                log_entry.pop("run_id", None)
                log_entry.pop("duration_ms", None)
                logs.append(log_entry)
        except (json.JSONDecodeError, AttributeError):
            # Not JSON, skip
            continue

    return logs


def run_pipeline_dry_run(pipeline_name: str, output_dir: Path) -> tuple[int, str, str]:
    """Run a pipeline with --dry-run and return exit code, stdout, stderr."""
    config_map = {
        "activity_chembl": "configs/pipelines/activity/activity_chembl.yaml",
        "assay_chembl": "configs/pipelines/assay/assay_chembl.yaml",
    }

    if pipeline_name not in config_map:
        return -1, "", f"Unknown pipeline: {pipeline_name}"

    config_path = ROOT / config_map[pipeline_name]

    cmd = [
        sys.executable,
        "-m",
        "bioetl.cli.main",
        pipeline_name,
        "--config",
        str(config_path),
        "--output-dir",
        str(output_dir),
        "--dry-run",
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
            cwd=ROOT,
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out after 120 seconds"
    except Exception as e:
        return -1, "", str(e)


def compare_logs(logs1: list[dict[str, Any]], logs2: list[dict[str, Any]]) -> tuple[bool, list[str]]:
    """Compare two sets of logs and return (are_identical, differences)."""
    differences: list[str] = []

    if len(logs1) != len(logs2):
        differences.append(f"Log count mismatch: run1 has {len(logs1)} logs, run2 has {len(logs2)} logs")

    min_len = min(len(logs1), len(logs2))
    for i in range(min_len):
        log1 = logs1[i]
        log2 = logs2[i]

        # Compare event types
        event1 = log1.get("event", "")
        event2 = log2.get("event", "")
        if event1 != event2:
            differences.append(f"Log {i}: event mismatch: '{event1}' vs '{event2}'")

        # Compare other fields (ignoring timestamps, run_id, duration_ms)
        keys1 = set(k for k in log1.keys() if k not in ("timestamp", "run_id", "duration_ms", "time"))
        keys2 = set(k for k in log2.keys() if k not in ("timestamp", "run_id", "duration_ms", "time"))

        if keys1 != keys2:
            diff_keys = keys1.symmetric_difference(keys2)
            differences.append(f"Log {i}: key mismatch: {diff_keys}")

        # Compare values for common keys
        common_keys = keys1.intersection(keys2)
        for key in common_keys:
            val1 = log1.get(key)
            val2 = log2.get(key)
            if val1 != val2:
                differences.append(f"Log {i}: {key} mismatch: {val1} vs {val2}")

    return len(differences) == 0, differences


def main() -> int:
    """Main entry point."""
    print("Running determinism check...\n")

    pipelines = ["activity_chembl", "assay_chembl"]
    results: dict[str, dict[str, Any]] = {}

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        for pipeline_name in pipelines:
            print(f"Testing {pipeline_name}...")

            # Run 1
            output_dir1 = temp_path / f"{pipeline_name}_run1"
            output_dir1.mkdir()
            exit_code1, stdout1, stderr1 = run_pipeline_dry_run(pipeline_name, output_dir1)

            if exit_code1 != 0:
                results[pipeline_name] = {
                    "deterministic": False,
                    "errors": [f"Run 1 failed with exit code {exit_code1}: {stderr1[:200]}"],
                }
                print(f"  ✗ Run 1 failed\n")
                continue

            # Run 2
            output_dir2 = temp_path / f"{pipeline_name}_run2"
            output_dir2.mkdir()
            exit_code2, stdout2, stderr2 = run_pipeline_dry_run(pipeline_name, output_dir2)

            if exit_code2 != 0:
                results[pipeline_name] = {
                    "deterministic": False,
                    "errors": [f"Run 2 failed with exit code {exit_code2}: {stderr2[:200]}"],
                }
                print(f"  ✗ Run 2 failed\n")
                continue

            # Extract and compare logs
            logs1 = extract_structured_logs(stdout1, stderr1)
            logs2 = extract_structured_logs(stdout2, stderr2)

            are_identical, differences = compare_logs(logs1, logs2)

            results[pipeline_name] = {
                "deterministic": are_identical,
                "run1_exit_code": exit_code1,
                "run2_exit_code": exit_code2,
                "run1_log_count": len(logs1),
                "run2_log_count": len(logs2),
                "differences": differences,
                "errors": [],
            }

            if are_identical:
                print(f"  ✓ Deterministic ({len(logs1)} logs per run)\n")
            else:
                print(f"  ✗ Non-deterministic: {len(differences)} difference(s)\n")
                for diff in differences[:5]:  # Show first 5 differences
                    print(f"    - {diff}")

    # Generate report
    AUDIT_RESULTS.mkdir(exist_ok=True)
    report_path = AUDIT_RESULTS / "DETERMINISM_CHECK_REPORT.md"

    total_deterministic = sum(1 for r in results.values() if r["deterministic"])
    total_non_deterministic = len(results) - total_deterministic

    with report_path.open("w", encoding="utf-8") as f:
        f.write("# Determinism Check Report\n\n")
        f.write("**Purpose**: Verify that pipeline runs produce identical structured logs.\n\n")
        f.write(f"**Total pipelines tested**: {len(results)}\n\n")
        f.write(f"- ✅ Deterministic: {total_deterministic}\n")
        f.write(f"- ❌ Non-deterministic: {total_non_deterministic}\n\n")

        for pipeline_name, result in results.items():
            f.write(f"## {pipeline_name}\n\n")
            f.write(f"**Status**: {'✅ Deterministic' if result['deterministic'] else '❌ Non-deterministic'}\n\n")

            if result.get("run1_exit_code") is not None:
                f.write(f"- Run 1 exit code: {result['run1_exit_code']}\n")
                f.write(f"- Run 2 exit code: {result['run2_exit_code']}\n")
                f.write(f"- Run 1 log count: {result['run1_log_count']}\n")
                f.write(f"- Run 2 log count: {result['run2_log_count']}\n\n")

            if result.get("differences"):
                f.write("**Differences**:\n\n")
                for diff in result["differences"]:
                    f.write(f"- {diff}\n")
                f.write("\n")

            if result.get("errors"):
                f.write("**Errors**:\n\n")
                for error in result["errors"]:
                    f.write(f"- {error}\n")
                f.write("\n")

    print(f"Report saved to {report_path}")

    # Return non-zero if any pipelines are non-deterministic
    return 1 if total_non_deterministic > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
