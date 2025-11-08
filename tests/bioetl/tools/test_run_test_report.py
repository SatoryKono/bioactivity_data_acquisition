"""Тесты для генерации отчётов pytest/coverage."""

from __future__ import annotations

import json
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from bioetl.tools import run_test_report
from tests.bioetl.core import test_report_artifacts


class _LoggerStub:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []
        self.warnings: list[tuple[str, dict[str, Any]]] = []
        self.errors: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: str, **context: Any) -> None:
        self.events.append((event, context))

    def warning(self, event: str, **context: Any) -> None:
        self.warnings.append((event, context))

    def error(self, event: str, **context: Any) -> None:
        self.errors.append((event, context))


@pytest.fixture
def report_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[_LoggerStub, Path]:
    """Изолированная среда для run_test_report."""

    logger = _LoggerStub()
    repo_root = tmp_path
    monkeypatch.setattr("bioetl.tools.run_test_report.REPO_ROOT", repo_root, raising=False)
    monkeypatch.setattr("bioetl.tools.run_test_report.UnifiedLogger.configure", lambda: None)
    monkeypatch.setattr("bioetl.tools.run_test_report.UnifiedLogger.bind", lambda **_: None)
    monkeypatch.setattr("bioetl.tools.run_test_report.UnifiedLogger.get", lambda *_: logger)
    monkeypatch.setattr("bioetl.tools.run_test_report.UnifiedLogger.stage", lambda *args, **kwargs: nullcontext())

    return logger, repo_root


@pytest.mark.unit
def test_blake2_digest_combines_parts() -> None:
    """Контрольная сумма стабильна для заданных частей."""

    digest_ab = run_test_report._blake2_digest([b"a", b"b"])
    digest_ba = run_test_report._blake2_digest([b"b", b"a"])
    digest_single = run_test_report._blake2_digest([b"ab"])

    assert digest_ab == digest_single
    assert digest_ab != digest_ba
    assert len(digest_ab) == 64


@pytest.mark.unit
def test_load_pytest_summary_parses_counts(tmp_path: Path) -> None:
    """Загрузка JSON-отчёта pytest."""

    payload = {"summary": {"collected": 3, "passed": 2, "failed": 1}}
    path = tmp_path / "report.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    collected, summary = run_test_report._load_pytest_summary(path)

    assert collected == 3
    assert summary["passed"] == 2


@pytest.mark.unit
def test_write_yaml_atomic_persists_payload(tmp_path: Path) -> None:
    """Атомарная запись YAML."""

    target = tmp_path / "meta.yaml"
    run_test_report._write_yaml_atomic(target, {"key": "value"})

    written = yaml.safe_load(target.read_text(encoding="utf-8"))
    assert written == {"key": "value"}


@pytest.mark.unit
def test_generate_test_report_success(
    report_env: tuple[_LoggerStub, Path], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Успешный сценарий генерации отчёта."""

    logger, repo_root = report_env

    timestamp_name = "20240101T000000Z"
    monkeypatch.setattr(
        "bioetl.tools.run_test_report.build_timestamp_directory_name",
        lambda *_: timestamp_name,
    )

    artifacts_holder: dict[str, Any] = {}

    def resolve_artifacts(root: Path) -> test_report_artifacts.TestReportArtifacts:
        artifacts = test_report_artifacts.resolve_artifact_paths(root)
        artifacts_holder["paths"] = artifacts
        return artifacts

    monkeypatch.setattr("bioetl.tools.run_test_report.resolve_artifact_paths", resolve_artifacts)
    monkeypatch.setattr("bioetl.tools.run_test_report._compute_pipeline_version", lambda: "1.2.3")
    monkeypatch.setattr("bioetl.tools.run_test_report._read_git_commit", lambda: "abcdef")
    monkeypatch.setattr("bioetl.tools.run_test_report._compute_config_hash", lambda: "deadbeef")

    def fake_run(cmd, cwd=None, check=False):
        artifacts = artifacts_holder["paths"]
        artifacts.pytest_report.write_text(
            json.dumps({"summary": {"collected": 2, "passed": 2}}),
            encoding="utf-8",
        )
        artifacts.coverage_xml.write_text("<coverage></coverage>", encoding="utf-8")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("bioetl.tools.run_test_report.subprocess.run", fake_run)

    result = run_test_report.generate_test_report(output_root=tmp_path)

    final_dir = tmp_path / timestamp_name
    meta_path = final_dir / test_report_artifacts.META_YAML_NAME

    meta_payload = yaml.safe_load(meta_path.read_text(encoding="utf-8"))

    assert result == 0
    assert meta_payload["pipeline_version"] == "1.2.3"
    assert meta_payload["git_commit"] == "abcdef"
    assert meta_payload["status"] == "passed"
    assert any(event[0] == "tests_succeeded" for event in logger.events)


@pytest.mark.unit
def test_generate_test_report_missing_pytest_json(
    report_env: tuple[_LoggerStub, Path], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Ошибка когда pytest-json не создан."""

    logger, _ = report_env
    monkeypatch.setattr(
        "bioetl.tools.run_test_report.build_timestamp_directory_name",
        lambda *_: "20240101T000000Z",
    )
    monkeypatch.setattr("bioetl.tools.run_test_report.resolve_artifact_paths", test_report_artifacts.resolve_artifact_paths)
    monkeypatch.setattr("bioetl.tools.run_test_report._compute_pipeline_version", lambda: "1.0.0")
    monkeypatch.setattr("bioetl.tools.run_test_report._read_git_commit", lambda: "commit")
    monkeypatch.setattr("bioetl.tools.run_test_report._compute_config_hash", lambda: "hash")
    monkeypatch.setattr(
        "bioetl.tools.run_test_report.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0),
    )

    result = run_test_report.generate_test_report(output_root=tmp_path)

    assert result == 1
    assert logger.errors and logger.errors[0][0] == "pytest_json_missing"

