"""Generate pytest and coverage report artifacts with metadata."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from collections.abc import Iterable
from typing import Any, Callable
from datetime import datetime, timezone
from hashlib import blake2b
from pathlib import Path
from uuid import uuid4

from bioetl.cli._io import atomic_write_yaml
from bioetl.core.logging import LogEvents, UnifiedLogger
from bioetl.tools.test_report_artifacts import (
    TEST_REPORTS_ROOT,
    TestReportMeta,
    build_timestamp_directory_name,
    resolve_artifact_paths,
)

__all__ = ["generate_test_report", "TEST_REPORTS_ROOT"]


REPO_ROOT = Path(__file__).resolve().parents[3]


def _blake2_digest(parts: Iterable[bytes], *, digest_size: int = 32) -> str:
    """Compute a deterministic BLAKE2 hash from byte chunks."""
    hasher = blake2b(digest_size=digest_size)
    for part in parts:
        hasher.update(part)
    return hasher.hexdigest()


def _load_pytest_summary(path: Path) -> tuple[int, dict[str, int]]:
    """Load pytest JSON report and extract collected count and summary stats."""
    data = json.loads(path.read_text(encoding="utf-8"))
    summary = data.get("summary", {})
    collected = int(summary.get("collected", 0))
    return collected, {str(k): int(v) for k, v in summary.items() if isinstance(v, int)}


def _compute_pipeline_version() -> str:
    """Resolve the BioETL package version for report metadata."""
    try:
        import importlib.metadata

        return importlib.metadata.version("bioetl")
    except Exception:  # pragma: no cover - fallback
        return "0.0.0-dev"


def _read_git_commit() -> str:
    """Read the current Git commit SHA for inclusion in metadata."""
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
    """Yield configuration files that contribute to the config hash."""
    configs_root = REPO_ROOT / "configs"
    if configs_root.exists():
        yield from sorted(configs_root.rglob("*.yaml"))
    yield REPO_ROOT / "pyproject.toml"


def _compute_config_hash() -> str:
    """Compute a BLAKE2 hash over config sources and pyproject."""
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


def generate_test_report(
    output_root: Path | None = None,
    *,
    subprocess_module: Any | None = None,
    logger_cls: type[UnifiedLogger] | None = None,
    datetime_module: type[datetime] | None = None,
    uuid4_fn: Callable[[], Any] | None = None,
    resolve_artifacts_fn: Callable[[Path], "TestReportArtifacts"] | None = None,
    repo_root: Path | None = None,
    pipeline_version_fn: Callable[[], str] | None = None,
    git_commit_fn: Callable[[], str] | None = None,
    config_hash_fn: Callable[[], str] | None = None,
) -> int:
    """Generate pytest/coverage artifacts and return the pytest exit code."""

    logger_type = logger_cls or UnifiedLogger
    now_datetime = datetime_module or datetime
    uuid_factory = uuid4_fn or uuid4
    resolver = resolve_artifacts_fn or resolve_artifact_paths
    repo_base = repo_root or REPO_ROOT
    runner = subprocess_module or subprocess

    logger_type.configure()
    run_id = uuid_factory().hex
    trace_id = uuid_factory().hex
    span_id = uuid_factory().hex[:16]

    logger_type.bind(
        run_id=run_id,
        pipeline="test-reporting",
        stage="bootstrap",
        dataset="test-reports",
        component="tools.run_test_report",
        trace_id=trace_id,
        span_id=span_id,
    )
    log = logger_type.get(__name__)

    target_root = output_root if output_root is not None else TEST_REPORTS_ROOT

    timestamp = now_datetime.now(timezone.utc)
    folder_name = build_timestamp_directory_name(timestamp)
    final_root = target_root / folder_name
    tmp_root = target_root / f".{folder_name}-{uuid_factory().hex}.tmp"

    if final_root.exists():
        log.error(LogEvents.TARGET_DIRECTORY_EXISTS, path=str(final_root))
        return 1

    log.info(LogEvents.PREPARING_DIRECTORIES, tmp_root=str(tmp_root), final_root=str(final_root))
    tmp_root.mkdir(parents=True, exist_ok=True)

    artifacts = resolver(tmp_root)
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

    log.info(LogEvents.RUNNING_PYTEST, command=pytest_cmd, cwd=str(repo_base))
    result = runner.run(pytest_cmd, cwd=repo_base, check=False) if hasattr(runner, "run") else runner(pytest_cmd)

    status = "passed" if result.returncode == 0 else "failed"
    logger_type.bind(stage="post-processing")
    log = logger_type.get(__name__)
    log.info(LogEvents.PYTEST_FINISHED, returncode=result.returncode, status=status)

    if not artifacts.pytest_report.exists():
        log.error(LogEvents.PYTEST_JSON_MISSING, path=str(artifacts.pytest_report))
        return max(result.returncode, 1)

    row_count, summary = _load_pytest_summary(artifacts.pytest_report)
    pytest_digest = _blake2_digest([artifacts.pytest_report.read_bytes()])

    compute_version = pipeline_version_fn or _compute_pipeline_version
    read_commit = git_commit_fn or _read_git_commit
    compute_config_hash = config_hash_fn or _compute_config_hash

    pipeline_version = compute_version()
    git_commit = read_commit()
    config_hash = compute_config_hash()
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

    log.info(LogEvents.WRITING_META,
        meta_path=str(artifacts.meta_yaml),
        status=status,
        row_count=row_count,
        coverage_xml=str(artifacts.coverage_xml),
    )
    atomic_write_yaml(meta_payload_with_summary, artifacts.meta_yaml)

    if html_dir.exists():
        shutil.rmtree(html_dir)

    log.info(LogEvents.FINALISING_OUTPUT, source=str(tmp_root), destination=str(final_root))
    tmp_root.rename(final_root)

    if status == "failed":
        log.warning(LogEvents.TESTS_FAILED, returncode=result.returncode)
    else:
        log.info(LogEvents.TESTS_SUCCEEDED)

    return result.returncode
