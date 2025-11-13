from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from bioetl.tools import link_check


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


def test_run_link_check_without_lychee(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(link_check, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(link_check, "PROJECT_ROOT", tmp_path)
    artifacts = tmp_path / "artifacts"
    monkeypatch.setattr(link_check, "ARTIFACTS_DIR", artifacts)

    def fake_run(*args: Any, **kwargs: Any):
        raise FileNotFoundError("lychee missing")

    monkeypatch.setattr(link_check.subprocess, "run", fake_run)
    exit_code = link_check.run_link_check()
    assert exit_code == 0
    report = artifacts / "link-check-report.md"
    assert "lychee is not installed" in report.read_text(encoding="utf-8")


def test_run_link_check_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(link_check, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(link_check, "PROJECT_ROOT", tmp_path)
    artifacts = tmp_path / "artifacts"
    monkeypatch.setattr(link_check, "ARTIFACTS_DIR", artifacts)

    class Result:
        def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    def fake_run(args: list[str], **kwargs: Any) -> Result:
        if args[:2] == ["lychee", "--version"]:
            return Result(0, "0.1")
        # emulate lychee writing report
        output_index = args.index("--output") + 1
        Path(args[output_index]).write_text("# ok", encoding="utf-8")
        return Result(0, "ok", "")

    monkeypatch.setattr(link_check.subprocess, "run", fake_run)
    exit_code = link_check.run_link_check(timeout_seconds=5)
    assert exit_code == 0
    assert (artifacts / "link-check-report.md").exists()

