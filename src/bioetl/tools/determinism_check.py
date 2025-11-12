"""Проверка детерминизма пайплайнов."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bioetl.core.logger import UnifiedLogger
from bioetl.core.log_events import LogEvents
from bioetl.tools import get_project_root

__all__ = [
    "DeterminismRunResult",
    "run_determinism_check",
]


PROJECT_ROOT = get_project_root()
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts"


def extract_structured_logs(stdout: str, stderr: str) -> list[dict[str, Any]]:
    """Извлекает структурированные JSON-логи из stdout/stderr."""

    logs: list[dict[str, Any]] = []
    all_output = stdout + "\n" + stderr

    for line in all_output.split("\n"):
        text = line.strip()
        if not text:
            continue
        try:
            if text.startswith("{") and text.endswith("}"):
                log_entry = json.loads(text)
                for field in ("timestamp", "run_id", "duration_ms", "time"):
                    log_entry.pop(field, None)
                logs.append(log_entry)
        except (json.JSONDecodeError, AttributeError):
            continue

    return logs


def run_pipeline_dry_run(pipeline_name: str, output_dir: Path) -> tuple[int, str, str]:
    """Запускает пайплайн с --dry-run и возвращает код выхода, stdout, stderr."""

    config_map = {
        "activity_chembl": "configs/pipelines/activity/activity_chembl.yaml",
        "assay_chembl": "configs/pipelines/assay/assay_chembl.yaml",
    }

    if pipeline_name not in config_map:
        return -1, "", f"Unknown pipeline: {pipeline_name}"

    config_path = PROJECT_ROOT / config_map[pipeline_name]

    cmd = [
        sys.executable,
        "-m",
        "bioetl.cli.app",
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
            cwd=PROJECT_ROOT,
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, "", "Command timed out after 120 seconds"
    except Exception as exc:  # noqa: BLE001
        return -1, "", str(exc)


def compare_logs(
    logs1: list[dict[str, Any]], logs2: list[dict[str, Any]]
) -> tuple[bool, list[str]]:
    """Сравнивает наборы логов и возвращает (идентичны, список отличий)."""

    differences: list[str] = []

    if len(logs1) != len(logs2):
        differences.append(
            f"Log count mismatch: run1 has {len(logs1)} logs, run2 has {len(logs2)} logs"
        )

    min_len = min(len(logs1), len(logs2))
    for index in range(min_len):
        log1 = logs1[index]
        log2 = logs2[index]

        event1 = log1.get("event", "")
        event2 = log2.get("event", "")
        if event1 != event2:
            differences.append(f"Log {index}: event mismatch: '{event1}' vs '{event2}'")

        keys1 = {k for k in log1 if k not in {"timestamp", "run_id", "duration_ms", "time"}}
        keys2 = {k for k in log2 if k not in {"timestamp", "run_id", "duration_ms", "time"}}

        if keys1 != keys2:
            diff_keys = keys1.symmetric_difference(keys2)
            differences.append(f"Log {index}: key mismatch: {sorted(diff_keys)}")

        for key in keys1.intersection(keys2):
            if log1.get(key) != log2.get(key):
                differences.append(
                    f"Log {index}: {key} mismatch: {log1.get(key)!r} vs {log2.get(key)!r}"
                )

    return len(differences) == 0, differences


@dataclass(frozen=True)
class DeterminismRunResult:
    """Результат проверки детерминизма."""

    pipeline_name: str
    deterministic: bool
    run1_exit_code: int | None
    run2_exit_code: int | None
    run1_log_count: int | None
    run2_log_count: int | None
    differences: tuple[str, ...]
    errors: tuple[str, ...]
    report_path: Path


def _write_report(report_path: Path, results: dict[str, DeterminismRunResult]) -> None:
    tmp = report_path.with_suffix(report_path.suffix + ".tmp")

    total_deterministic = sum(1 for item in results.values() if item.deterministic)
    total_non_deterministic = len(results) - total_deterministic

    with tmp.open("w", encoding="utf-8") as handle:
        handle.write("# Determinism Check Report\n\n")
        handle.write(
            "**Purpose**: Verify that pipeline runs produce identical structured logs.\n\n"
        )
        handle.write(f"**Total pipelines tested**: {len(results)}\n\n")
        handle.write(f"- ✅ Deterministic: {total_deterministic}\n")
        handle.write(f"- ❌ Non-deterministic: {total_non_deterministic}\n\n")

        for pipeline_name, item in results.items():
            handle.write(f"## {pipeline_name}\n\n")
            status = "✅ Deterministic" if item.deterministic else "❌ Non-deterministic"
            handle.write(f"**Status**: {status}\n\n")

            if item.run1_exit_code is not None:
                handle.write(f"- Run 1 exit code: {item.run1_exit_code}\n")
                handle.write(f"- Run 2 exit code: {item.run2_exit_code}\n")
                handle.write(f"- Run 1 log count: {item.run1_log_count}\n")
                handle.write(f"- Run 2 log count: {item.run2_log_count}\n\n")

            if item.differences:
                handle.write("**Differences**:\n\n")
                for diff in item.differences:
                    handle.write(f"- {diff}\n")
                handle.write("\n")

            if item.errors:
                handle.write("**Errors**:\n\n")
                for error in item.errors:
                    handle.write(f"- {error}\n")
                handle.write("\n")

    tmp.replace(report_path)


def run_determinism_check(
    pipelines: tuple[str, ...] | None = None,
) -> dict[str, DeterminismRunResult]:
    """Запускает проверку детерминизма и возвращает результаты."""

    UnifiedLogger.configure()
    log = UnifiedLogger.get(__name__)

    target_pipelines = pipelines or ("activity_chembl", "assay_chembl")
    log.info(LogEvents.DETERMINISM_CHECK_START, pipelines=target_pipelines)

    results: dict[str, DeterminismRunResult] = {}

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        for pipeline_name in target_pipelines:
            log.info(LogEvents.RUNNING_PIPELINE_CHECK, pipeline=pipeline_name)

            output_dir1 = temp_path / f"{pipeline_name}_run1"
            output_dir1.mkdir()
            exit_code1, stdout1, stderr1 = run_pipeline_dry_run(pipeline_name, output_dir1)

            if exit_code1 != 0:
                error_message = f"Run 1 failed with exit code {exit_code1}: {stderr1[:200]}"
                log.warning(LogEvents.PIPELINE_RUN_FAILED, pipeline=pipeline_name, attempt=1, error=error_message
                )
                results[pipeline_name] = DeterminismRunResult(
                    pipeline_name=pipeline_name,
                    deterministic=False,
                    run1_exit_code=exit_code1,
                    run2_exit_code=None,
                    run1_log_count=None,
                    run2_log_count=None,
                    differences=(),
                    errors=(error_message,),
                    report_path=ARTIFACTS_DIR / "DETERMINISM_CHECK_REPORT.md",
                )
                continue

            output_dir2 = temp_path / f"{pipeline_name}_run2"
            output_dir2.mkdir()
            exit_code2, stdout2, stderr2 = run_pipeline_dry_run(pipeline_name, output_dir2)

            if exit_code2 != 0:
                error_message = f"Run 2 failed with exit code {exit_code2}: {stderr2[:200]}"
                log.warning(LogEvents.PIPELINE_RUN_FAILED, pipeline=pipeline_name, attempt=2, error=error_message
                )
                results[pipeline_name] = DeterminismRunResult(
                    pipeline_name=pipeline_name,
                    deterministic=False,
                    run1_exit_code=exit_code1,
                    run2_exit_code=exit_code2,
                    run1_log_count=None,
                    run2_log_count=None,
                    differences=(),
                    errors=(error_message,),
                    report_path=ARTIFACTS_DIR / "DETERMINISM_CHECK_REPORT.md",
                )
                continue

            logs1 = extract_structured_logs(stdout1, stderr1)
            logs2 = extract_structured_logs(stdout2, stderr2)
            are_identical, differences = compare_logs(logs1, logs2)

            if are_identical:
                log.info(LogEvents.PIPELINE_DETERMINISTIC, pipeline=pipeline_name, log_count=len(logs1))
            else:
                log.warning(LogEvents.PIPELINE_NOT_DETERMINISTIC,
                    pipeline=pipeline_name,
                    differences=len(differences),
                )

            results[pipeline_name] = DeterminismRunResult(
                pipeline_name=pipeline_name,
                deterministic=are_identical,
                run1_exit_code=exit_code1,
                run2_exit_code=exit_code2,
                run1_log_count=len(logs1),
                run2_log_count=len(logs2),
                differences=tuple(differences),
                errors=(),
                report_path=ARTIFACTS_DIR / "DETERMINISM_CHECK_REPORT.md",
            )

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = ARTIFACTS_DIR / "DETERMINISM_CHECK_REPORT.md"
    _write_report(report_path, results)
    log.info(LogEvents.DETERMINISM_CHECK_REPORT_WRITTEN, path=str(report_path))

    return results
