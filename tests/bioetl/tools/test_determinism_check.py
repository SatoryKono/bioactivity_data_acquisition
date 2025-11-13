from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from bioetl.tools import determinism_check


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

