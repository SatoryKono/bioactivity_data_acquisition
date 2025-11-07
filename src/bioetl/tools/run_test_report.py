"""Генерация артефактов отчёта pytest/coverage."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from hashlib import blake2b
from pathlib import Path
from uuid import uuid4

import yaml

from bioetl.core.logger import UnifiedLogger
from bioetl.core.test_report_artifacts import (
    TEST_REPORTS_ROOT,
    TestReportMeta,
    build_timestamp_directory_name,
    resolve_artifact_paths,
)

__all__ = ["generate_test_report"]


REPO_ROOT = Path(__file__).resolve().parents[3]


def _blake2_digest(parts: Iterable[bytes], *, digest_size: int = 32) -> str:
    hasher = blake2b(digest_size=digest_size)
    for part in parts:
        hasher.update(part)
    return hasher.hexdigest()


def _load_pytest_summary(path: Path) -> tuple[int, dict[str, int]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    summary = data.get("summary", {})
    collected = int(summary.get("collected", 0))
    return collected, {str(k): int(v) for k, v in summary.items() if isinstance(v, int)}


def _compute_pipeline_version() -> str:
    try:
        import importlib.metadata

        return importlib.metadata.version("bioetl")
    except Exception:  # pragma: no cover - fallback
        return "0.0.0-dev"


def _read_git_commit() -> str:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        )
    except Exception:  # pragma: no cover - fallback
        return "UNKNOWN"
    return completed.stdout.strip()


def _iter_config_sources() -> Iterable[Path]:
    configs_root = REPO_ROOT / "configs"
    if configs_root.exists():
        yield from sorted(configs_root.rglob("*.yaml"))
    yield REPO_ROOT / "pyproject.toml"


def _compute_config_hash() -> str:
    parts: list[bytes] = []
    for source in _iter_config_sources():
        if not source.exists():
            continue
        relative = source.relative_to(REPO_ROOT)
        parts.append(str(relative).encode("utf-8"))
        parts.append(b"\0")
        parts.append(source.read_bytes())
        parts.append(b"\0")
    return _blake2_digest(parts)


def _write_yaml_atomic(path: Path, payload: Mapping[str, object]) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False, allow_unicode=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)


def generate_test_report(output_root: Path | None = None) -> int:
    """Генерирует отчёты pytest/coverage и возвращает код выхода pytest."""

    UnifiedLogger.configure()
    run_id = uuid4().hex
    trace_id = uuid4().hex
    span_id = uuid4().hex[:16]

    UnifiedLogger.bind(
        run_id=run_id,
        pipeline="test-reporting",
        stage="bootstrap",
        dataset="test-reports",
        component="tools.run_test_report",
        trace_id=trace_id,
        span_id=span_id,
    )
    log = UnifiedLogger.get(__name__)

    target_root = output_root if output_root is not None else TEST_REPORTS_ROOT

    timestamp = datetime.now(timezone.utc)
    folder_name = build_timestamp_directory_name(timestamp)
    final_root = target_root / folder_name
    tmp_root = target_root / f".{folder_name}-{uuid4().hex}.tmp"

    if final_root.exists():
        log.error("target_directory_exists", path=str(final_root))
        return 1

    log.info("preparing_directories", tmp_root=str(tmp_root), final_root=str(final_root))
    tmp_root.mkdir(parents=True, exist_ok=True)

    artifacts = resolve_artifact_paths(tmp_root)
    html_dir = tmp_root / "coverage-html"
    html_dir.mkdir(parents=True, exist_ok=True)

    pytest_cmd = [
        sys.executable,
        "-m",
        "pytest",
        "--json-report",
        f"--json-report-file={artifacts.pytest_report}",
        f"--cov-report=xml:{artifacts.coverage_xml}",
        f"--cov-report=html:{html_dir}",
    ]

    log.info("running_pytest", command=pytest_cmd, cwd=str(REPO_ROOT))
    result = subprocess.run(pytest_cmd, cwd=REPO_ROOT, check=False)

    status = "passed" if result.returncode == 0 else "failed"
    UnifiedLogger.bind(stage="post-processing")
    log = UnifiedLogger.get(__name__)
    log.info("pytest_finished", returncode=result.returncode, status=status)

    if not artifacts.pytest_report.exists():
        log.error("pytest_json_missing", path=str(artifacts.pytest_report))
        return max(result.returncode, 1)

    row_count, summary = _load_pytest_summary(artifacts.pytest_report)
    pytest_digest = _blake2_digest([artifacts.pytest_report.read_bytes()])

    pipeline_version = _compute_pipeline_version()
    git_commit = _read_git_commit()
    config_hash = _compute_config_hash()
    business_key_hash = _blake2_digest(
        [
            pipeline_version.encode("utf-8"),
            b"|",
            git_commit.encode("utf-8"),
            b"|",
            config_hash.encode("utf-8"),
        ]
    )

    generated_at_utc = timestamp.isoformat().replace("+00:00", "Z")

    meta = TestReportMeta(
        pipeline_version=pipeline_version,
        git_commit=git_commit,
        config_hash=config_hash,
        row_count=row_count,
        generated_at_utc=generated_at_utc,
        blake2_checksum=pytest_digest,
        business_key_hash=business_key_hash,
        status=status,
    )

    meta_payload = meta.to_ordered_dict()
    meta_payload_with_summary: dict[str, str | int | dict[str, int]] = {
        **meta_payload,
        "summary": summary,
    }

    log.info(
        "writing_meta",
        meta_path=str(artifacts.meta_yaml),
        status=status,
        row_count=row_count,
        coverage_xml=str(artifacts.coverage_xml),
    )
    _write_yaml_atomic(artifacts.meta_yaml, meta_payload_with_summary)

    if html_dir.exists():
        shutil.rmtree(html_dir)

    log.info("finalising_output", source=str(tmp_root), destination=str(final_root))
    tmp_root.rename(final_root)

    if status == "failed":
        log.warning("tests_failed", returncode=result.returncode)
    else:
        log.info("tests_succeeded")

    return result.returncode
