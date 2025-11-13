"""Дополнительные тесты для determinism_check."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from bioetl.tools import determinism_check as module


def test_extract_structured_logs_filters_fields() -> None:
    log_line = '{"event": "step", "timestamp": "now", "extra": 1}'
    logs = module.extract_structured_logs(log_line, "")

    assert logs == [{"event": "step", "extra": 1}]


def test_compare_logs_reports_differences() -> None:
    logs1 = [{"event": "start", "value": 1}]
    logs2 = [{"event": "finish", "value": 2}]

    deterministic, diffs = module.compare_logs(logs1, logs2)

    assert not deterministic
    assert any("event mismatch" in diff for diff in diffs)


class _DummyLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: str, **payload: Any) -> None:
        self.events.append((event, payload))

    def warning(self, event: str, **payload: Any) -> None:
        self.events.append((event, payload))

    def error(self, event: str, **payload: Any) -> None:
        self.events.append((event, payload))

    def bind(self, **_: Any) -> "_DummyLogger":
        return self


class _DummyUnifiedLogger:
    def __init__(self) -> None:
        self.logger = _DummyLogger()

    def configure(self) -> None:
        return None

    def get(self, _: str) -> _DummyLogger:
        return self.logger


class _FakeTempDir:
    def __init__(self, path: Path) -> None:
        self.path = path

    def __enter__(self) -> Path:
        self.path.mkdir(parents=True, exist_ok=True)
        return self.path

    def __exit__(self, *_: Any) -> None:
        return None


def test_run_determinism_check_handles_mixed_results(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "UnifiedLogger", _DummyUnifiedLogger())
    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(module, "ARTIFACTS_DIR", tmp_path / "artifacts")

    calls: list[tuple[str, Path]] = []
    run_counts: dict[str, int] = {}

    def fake_run(pipeline_name: str, output_dir: Path) -> tuple[int, str, str]:
        calls.append((pipeline_name, output_dir))
        run_counts[pipeline_name] = run_counts.get(pipeline_name, 0) + 1
        if pipeline_name == "ok":
            stdout = '{"event": "start"}\n{"event": "end"}'
            return 0, stdout, ""
        if pipeline_name == "fail-first":
            return 1, "", "error"
        if run_counts[pipeline_name] == 1:
            return 0, '{"event": "only"}', ""
        return 0, '{"event": "changed"}', ""

    monkeypatch.setattr(module, "run_pipeline_dry_run", fake_run)
    monkeypatch.setattr(module.tempfile, "TemporaryDirectory", lambda: _FakeTempDir(tmp_path / "tmp"))

    results = module.run_determinism_check(pipelines=("ok", "fail-first", "non-deterministic"))

    assert set(results.keys()) == {"ok", "fail-first", "non-deterministic"}
    assert results["ok"].deterministic
    assert not results["fail-first"].deterministic
    assert not results["non-deterministic"].deterministic
    report = module.ARTIFACTS_DIR / "DETERMINISM_CHECK_REPORT.md"
    assert report.exists()

