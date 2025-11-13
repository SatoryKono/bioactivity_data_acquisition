from __future__ import annotations

from pathlib import Path

import pytest

from bioetl.tools.remove_type_ignore import (
    TYPE_IGNORE_PATTERN,
    _cleanse_file,
    _iter_python_files,
    remove_type_ignore,
)


class DummyLogger:
    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict[str, object]]] = []

    def info(self, event: str, **context: object) -> None:
        self.records.append(("info", event, context))

    def warning(self, event: str, **context: object) -> None:
        self.records.append(("warning", event, context))

    def error(self, event: str, **context: object) -> None:
        self.records.append(("error", event, context))


class DummyUnifiedLogger:
    def __init__(self, logger: DummyLogger | None = None) -> None:
        self.logger = logger or DummyLogger()

    def configure(self) -> None:
        return None

    def get(self, name: str | None = None) -> DummyLogger:
        return self.logger


def test_iter_python_files_skips_virtualenv(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    python_file = root / "module.py"
    python_file.write_text("x = 1", encoding="utf-8")
    (root / ".venv").mkdir()
    venv_file = root / ".venv" / "ignored.py"
    venv_file.write_text("y = 2", encoding="utf-8")

    discovered = list(_iter_python_files(root))
    assert python_file in discovered
    assert venv_file not in discovered


def test_cleanse_file_removes_type_ignore(tmp_path: Path) -> None:
    target = tmp_path / "module.py"
    target.write_text("value = cast(int, foo)  # type: ignore[assignment]\n", encoding="utf-8")

    removed = _cleanse_file(target)
    assert removed == 1
    assert "type: ignore" not in target.read_text(encoding="utf-8")


def test_type_ignore_pattern_matches_variants() -> None:
    cases = [
        "# type: ignore",
        "# type: ignore[attr-defined]",
        "# type: ignore[attr-defined]  ",
        "#    type:    ignore",
    ]
    for sample in cases:
        assert TYPE_IGNORE_PATTERN.search(f"foo {sample}") is not None


def test_remove_type_ignore_counts_and_logs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = tmp_path / "repository"
    package_dir = repo_root / "pkg"
    package_dir.mkdir(parents=True)
    sample = package_dir / "mod.py"
    sample.write_text(
        "value = object()  # type: ignore[attr-defined]\nother = 1  # okay\n", encoding="utf-8"
    )

    dummy_logger = DummyLogger()
    dummy_unified_logger = DummyUnifiedLogger()
    dummy_unified_logger.logger = dummy_logger

    monkeypatch.setattr("bioetl.tools.remove_type_ignore.UnifiedLogger", dummy_unified_logger)
    monkeypatch.setattr("bioetl.tools.remove_type_ignore.get_project_root", lambda: repo_root)

    removed = remove_type_ignore()

    assert removed == 1
    assert "type: ignore" not in sample.read_text(encoding="utf-8")
    assert dummy_logger.records[-1][0] == "info"

