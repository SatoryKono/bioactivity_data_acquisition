"""Дополнительные тесты для link_check."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from bioetl.tools import link_check as module


class _DummyLogger:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: str, **payload: Any) -> None:
        self.records.append((event, payload))

    def warning(self, event: str, **payload: Any) -> None:
        self.records.append((event, payload))

    def error(self, event: str, **payload: Any) -> None:
        self.records.append((event, payload))

    def bind(self, **_: Any) -> "_DummyLogger":
        return self


class _DummyUnifiedLogger:
    def __init__(self) -> None:
        self.logger = _DummyLogger()

    def configure(self) -> None:
        return None

    def get(self, _: str) -> _DummyLogger:
        return self.logger


def test_run_link_check_handles_missing_lychee(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "UnifiedLogger", _DummyUnifiedLogger())
    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(module, "ARTIFACTS_DIR", tmp_path / "artifacts")

    def fake_run(*_: Any, **__: Any) -> Any:
        raise FileNotFoundError("missing")

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    exit_code = module.run_link_check(timeout_seconds=10)

    assert exit_code == 0
    report = module.ARTIFACTS_DIR / "link-check-report.md"
    assert report.exists()
    assert "lychee is not installed" in report.read_text(encoding="utf-8")


def test_run_link_check_propagates_exit_code(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "UnifiedLogger", _DummyUnifiedLogger())
    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(module, "ARTIFACTS_DIR", tmp_path / "artifacts")

    class _FakeCompleted:
        def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> _FakeCompleted:
        calls.append(args)
        if args[:2] == ["lychee", "--version"]:
            return _FakeCompleted(0, stdout="lychee 1.0")
        return _FakeCompleted(3, stdout="done", stderr="error")

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    code = module.run_link_check(timeout_seconds=5)

    assert code == 3
    assert calls[0][:2] == ["lychee", "--version"]
    assert calls[1][0] == "lychee"


def test_run_link_check_handles_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "UnifiedLogger", _DummyUnifiedLogger())
    monkeypatch.setattr(module, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(module, "ARTIFACTS_DIR", tmp_path / "artifacts")

    class _FakeCompleted:
        returncode = 0

    def fake_run(*args: Any, **kwargs: Any) -> _FakeCompleted:
        if args[0][:2] == ["lychee", "--version"]:
            return _FakeCompleted()
        raise module.subprocess.TimeoutExpired(cmd=args[0], timeout=1)

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    exit_code = module.run_link_check(timeout_seconds=1)

    assert exit_code == 1

