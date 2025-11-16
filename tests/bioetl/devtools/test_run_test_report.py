from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import pytest
import yaml

import bioetl.devtools.cli_run_test_report as run_test_report
from bioetl.tools.test_report_artifacts import TestReportArtifacts


class StubLogger:
    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict[str, Any]]] = []

    def info(self, event: str, **context: Any) -> None:
        self.records.append(("info", event, context))

    def warning(self, event: str, **context: Any) -> None:
        self.records.append(("warning", event, context))

    def error(self, event: str, **context: Any) -> None:
        self.records.append(("error", event, context))


class StubUnifiedLogger:
    def __init__(self) -> None:
        self.logger = StubLogger()
        self.bound: list[dict[str, Any]] = []

    def configure(self) -> None:
        return None

    def bind(self, **context: Any) -> None:
        self.bound.append(context)

    def get(self, name: str | None = None) -> StubLogger:
        return self.logger


@dataclass
class FakeCompletedProcess:
    returncode: int = 0


class UUIDIterator:
    def __init__(self, values: Iterable[str]) -> None:
        self._iterator: Iterator[str] = iter(values)

    def __call__(self) -> "_UUIDWrapper":
        value = next(self._iterator)
        return _UUIDWrapper(value)


class _UUIDWrapper:
    def __init__(self, value: str) -> None:
        self._value = value

    @property
    def hex(self) -> str:
        return self._value


class FakeDateTime(run_test_report.datetime):  # type: ignore[misc]
    @classmethod
    def now(cls, tz: run_test_report.timezone | None = None):  # type: ignore[override]
        return super().fromisoformat("2024-01-01T00:00:00+00:00")


def test_blake2_digest_concatenates_bytes() -> None:
    digest = run_test_report._blake2_digest([b"a", b"b"])  # pylint: disable=protected-access
    assert digest == run_test_report._blake2_digest([b"ab"])  # pylint: disable=protected-access


def test_load_pytest_summary_parses_counts(tmp_path: Path) -> None:
    payload = {"summary": {"collected": 5, "passed": 5, "duration": 0.1}}
    target = tmp_path / "report.json"
    target.write_text(json.dumps(payload), encoding="utf-8")

    collected, summary = run_test_report._load_pytest_summary(target)  # pylint: disable=protected-access

    assert collected == 5
    assert summary == {"collected": 5, "passed": 5}


def test_compute_config_hash_uses_sorted_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    configs_dir = repo_root / "configs"
    configs_dir.mkdir(parents=True)
    file_a = configs_dir / "a.yaml"
    file_b = configs_dir / "nested" / "b.yaml"
    file_b.parent.mkdir()
    file_a.write_text("key: 1\n", encoding="utf-8")
    file_b.write_text("value: 2\n", encoding="utf-8")
    (repo_root / "pyproject.toml").write_text("[tool]\n", encoding="utf-8")

    monkeypatch.setattr(run_test_report, "REPO_ROOT", repo_root)

    digest = run_test_report._compute_config_hash()  # pylint: disable=protected-access

    assert len(digest) == 64


def test_write_yaml_atomic_creates_file(tmp_path: Path) -> None:
    payload = {"key": "value", "number": 1}
    target = tmp_path / "meta.yaml"

    run_test_report._write_yaml_atomic(target, payload)  # pylint: disable=protected-access

    assert yaml.safe_load(target.read_text(encoding="utf-8")) == payload


def test_generate_test_report_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_root = tmp_path / "reports"
    facade = StubUnifiedLogger()
    monkeypatch.setattr(run_test_report, "UnifiedLogger", facade)
    monkeypatch.setattr(run_test_report, "_compute_pipeline_version", lambda: "1.0.0")  # type: ignore[attr-defined]
    monkeypatch.setattr(run_test_report, "_read_git_commit", lambda: "deadbeef")  # type: ignore[attr-defined]
    monkeypatch.setattr(run_test_report, "_compute_config_hash", lambda: "hash123")  # type: ignore[attr-defined]
    monkeypatch.setattr(run_test_report, "datetime", FakeDateTime)
    monkeypatch.setattr(run_test_report, "uuid4", UUIDIterator(["runid" * 8, "trace" * 8, "span" * 8, "temp" * 8]))

    artifacts_holder: list[TestReportArtifacts] = []

    original_resolve = run_test_report.resolve_artifact_paths

    def capture_artifacts(root: Path) -> TestReportArtifacts:
        artifacts = original_resolve(root)
        artifacts_holder.append(artifacts)
        return artifacts

    monkeypatch.setattr(run_test_report, "resolve_artifact_paths", capture_artifacts)

    def fake_subprocess_run(cmd: list[str], cwd: Path | None = None, check: bool = False) -> FakeCompletedProcess:
        assert cmd[1:3] == ["-m", "pytest"]
        artifacts = artifacts_holder[0]
        artifacts.pytest_report.write_text(json.dumps({"summary": {"collected": 1, "passed": 1}}), encoding="utf-8")
        artifacts.coverage_xml.write_text("<coverage></coverage>", encoding="utf-8")
        return FakeCompletedProcess(returncode=0)

    monkeypatch.setattr(run_test_report.subprocess, "run", fake_subprocess_run)

    exit_code = run_test_report.generate_test_report(output_root)

    assert exit_code == 0
    final_dir = next(output_root.iterdir())
    meta_path = final_dir / "meta.yaml"
    assert meta_path.exists()
    meta = yaml.safe_load(meta_path.read_text(encoding="utf-8"))
    assert meta["pipeline_version"] == "1.0.0"
    assert meta["git_commit"] == "deadbeef"
    assert meta["config_hash"] == "hash123"
    assert meta["row_count"] == 1
    assert meta["summary"] == {"collected": 1, "passed": 1}


def test_generate_test_report_fails_when_directory_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_root = tmp_path / "reports"
    output_root.mkdir()
    facade = StubUnifiedLogger()
    monkeypatch.setattr(run_test_report, "UnifiedLogger", facade)
    monkeypatch.setattr(run_test_report, "uuid4", UUIDIterator(["runid" * 8, "trace" * 8, "span" * 8, "temp" * 8]))
    monkeypatch.setattr(run_test_report, "datetime", FakeDateTime)

    folder_name = run_test_report.build_timestamp_directory_name(FakeDateTime.now())  # type: ignore[arg-type]
    final_dir = output_root / folder_name
    final_dir.mkdir()

    exit_code = run_test_report.generate_test_report(output_root)

    assert exit_code == 1

