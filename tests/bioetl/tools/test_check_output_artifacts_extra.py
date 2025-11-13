"""Тесты для check_output_artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from bioetl.tools import check_output_artifacts as module


class _DummyLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: str, **payload: Any) -> None:
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


@pytest.fixture(autouse=True)
def patch_logger(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "UnifiedLogger", _DummyUnifiedLogger())


def test_check_output_artifacts_detects_tracked(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo_root = tmp_path
    output_dir = repo_root / "data" / "output"
    output_dir.mkdir(parents=True)
    (output_dir / "file.txt").write_text("data", encoding="utf-8")

    monkeypatch.setattr(module, "get_project_root", lambda: repo_root)
    monkeypatch.setattr(module, "_git_ls_files", lambda path: [Path("data/output/file.txt")])
    monkeypatch.setattr(module, "_git_diff_cached", lambda path: [])

    errors = module.check_output_artifacts(max_bytes=100)

    assert any("Tracked artifacts detected" in error for error in errors)


def test_check_output_artifacts_detects_staged_and_oversized(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo_root = tmp_path
    output_dir = repo_root / "data" / "output"
    output_dir.mkdir(parents=True)
    big_file = output_dir / "big.bin"
    big_file.write_bytes(b"x" * 200)

    monkeypatch.setattr(module, "get_project_root", lambda: repo_root)
    monkeypatch.setattr(module, "_git_ls_files", lambda path: [])
    monkeypatch.setattr(module, "_git_diff_cached", lambda path: [Path("data/output/new.txt")])

    errors = module.check_output_artifacts(max_bytes=50)

    assert any("New artifacts staged" in error for error in errors)
    assert any("Large files found" in error for error in errors)


def test_check_output_artifacts_no_issues(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    repo_root = tmp_path
    (repo_root / "data" / "output").mkdir(parents=True)

    monkeypatch.setattr(module, "get_project_root", lambda: repo_root)
    monkeypatch.setattr(module, "_git_ls_files", lambda path: [])
    monkeypatch.setattr(module, "_git_diff_cached", lambda path: [])

    errors = module.check_output_artifacts(max_bytes=100)

    assert errors == []


def test_git_helpers_parse_output(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Result:
        def __init__(self, output: str) -> None:
            self.stdout = output

    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: _Result("path/one\npath/two\n"),
    )
    files = module._git_ls_files("path")
    assert files == [Path("path/one"), Path("path/two")]

    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: _Result("diff/one\n"),
    )
    staged = module._git_diff_cached("path")
    assert staged == [Path("diff/one")]

