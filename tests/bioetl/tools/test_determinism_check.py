from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from bioetl.cli.tools._logic import cli_determinism_check as determinism_check


class DummyLogger:
    def info(self, *args: Any, **kwargs: Any) -> None:
        pass

    def warning(self, *args: Any, **kwargs: Any) -> None:
        pass

    def error(self, *args: Any, **kwargs: Any) -> None:
        pass


class DummyUnifiedLogger:
    @staticmethod
    def configure() -> None:
        pass

    @staticmethod
    def get(_: str) -> DummyLogger:
        return DummyLogger()


def test_extract_structured_logs_and_compare() -> None:
    sample_logs = [
        {"event": "start", "payload": 1, "timestamp": "ignored"},
        {"event": "finish", "payload": 2, "run_id": "ignored"},
    ]
    output = "\n".join(json.dumps(item) for item in sample_logs)
    logs = determinism_check.extract_structured_logs(output, "")
    assert len(logs) == 2
    assert "timestamp" not in logs[0]

    identical, diffs = determinism_check.compare_logs(logs, logs)
    assert identical
    assert not diffs

    mismatch, diffs = determinism_check.compare_logs(logs, logs[:-1])
    assert not mismatch
    assert "Log count mismatch" in diffs[0]


def test_run_determinism_check_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(determinism_check, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(determinism_check, "PROJECT_ROOT", tmp_path)
    artifacts = tmp_path / "artifacts"
    monkeypatch.setattr(determinism_check, "ARTIFACTS_DIR", artifacts)

    def fake_run_pipeline_dry_run(name: str, out: Path) -> tuple[int, str, str]:
        entries = [{"event": f"{name}-stage", "value": idx} for idx in range(2)]
        stdout = "\n".join(json.dumps(entry) for entry in entries)
        return 0, stdout, ""

    monkeypatch.setattr(determinism_check, "run_pipeline_dry_run", fake_run_pipeline_dry_run)
    results = determinism_check.run_determinism_check(pipelines=("activity_chembl",))
    assert results["activity_chembl"].deterministic
    assert results["activity_chembl"].differences == ()
    report = artifacts / "DETERMINISM_CHECK_REPORT.md"
    assert report.exists()
    content = report.read_text(encoding="utf-8")
    assert "Determinism Check Report" in content


def test_run_determinism_check_handles_failures(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(determinism_check, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(determinism_check, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(determinism_check, "ARTIFACTS_DIR", tmp_path / "artifacts")

    def failing_run_pipeline(name: str, out: Path) -> tuple[int, str, str]:
        return -1, "", f"failure for {name}"

    monkeypatch.setattr(determinism_check, "run_pipeline_dry_run", failing_run_pipeline)
    results = determinism_check.run_determinism_check(pipelines=("activity_chembl",))
    assert not results["activity_chembl"].deterministic
    assert results["activity_chembl"].errors


def test_extract_structured_logs_ignores_invalid_lines() -> None:
    stdout = '{"event":"ok"}\nnot-json\n'
    stderr = '{"event":"done","timestamp":"2024"}'
    logs = determinism_check.extract_structured_logs(stdout, stderr)
    assert [log["event"] for log in logs] == ["ok", "done"]


def test_compare_logs_identifies_differences() -> None:
    log1 = [{"event": "start", "value": 1}]
    log2 = [{"event": "finish", "value": 2, "extra": 3}]
    identical, diffs = determinism_check.compare_logs(log1, log2)
    assert not identical
    assert any("event mismatch" in diff for diff in diffs)
    assert any("key mismatch" in diff for diff in diffs)


def test_run_pipeline_dry_run_handles_unknown() -> None:
    code, _, stderr = determinism_check.run_pipeline_dry_run("unknown", Path("out"))
    assert code == -1
    assert "Unknown pipeline" in stderr


def test_run_pipeline_dry_run_handles_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[object]:
        raise subprocess.TimeoutExpired(cmd=args[0], timeout=1)

    monkeypatch.setattr(determinism_check.subprocess, "run", fake_run)
    code, stdout, stderr = determinism_check.run_pipeline_dry_run("activity_chembl", Path("out"))
    assert code == -1
    assert "timed out" in stderr
    assert stdout == ""


def test_run_determinism_check_second_run_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(determinism_check, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(determinism_check, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(determinism_check, "ARTIFACTS_DIR", tmp_path / "artifacts")

    def run_pipeline(name: str, out: Path) -> tuple[int, str, str]:
        if "run2" in out.name:
            return 2, "", "crash"
        entries = [{"event": "ok"}]
        stdout = "\n".join(json.dumps(entry) for entry in entries)
        return 0, stdout, ""

    monkeypatch.setattr(determinism_check, "run_pipeline_dry_run", run_pipeline)
    results = determinism_check.run_determinism_check(pipelines=("activity_chembl",))
    result = results["activity_chembl"]
    assert not result.deterministic
    assert result.errors == ("Run 2 failed with exit code 2: crash",)

