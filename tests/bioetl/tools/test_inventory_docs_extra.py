"""Дополнительные тесты для inventory_docs."""

from __future__ import annotations

from pathlib import Path

import pytest

from bioetl.tools import inventory_docs as module


class _DummyLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def info(self, event: str, **payload: object) -> None:
        self.events.append((event, payload))

    def bind(self, **_: object) -> "_DummyLogger":
        return self


class _DummyUnifiedLogger:
    def __init__(self) -> None:
        self.logger = _DummyLogger()

    def configure(self) -> None:
        return None

    def get(self, _: str) -> _DummyLogger:
        return self.logger


def test_collect_markdown_files_uses_custom_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    docs_root = tmp_path / "docs"
    docs_root.mkdir()
    (docs_root / "a.md").write_text("# A", encoding="utf-8")
    (docs_root / "b.txt").write_text("skip", encoding="utf-8")

    monkeypatch.setattr(module, "get_project_root", lambda: tmp_path)

    files = module.collect_markdown_files(docs_root=docs_root)

    assert files == (docs_root / "a.md",)


def test_write_inventory_creates_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    inventory_path = tmp_path / "inv.txt"
    hashes_path = tmp_path / "hashes.txt"
    docs_root = tmp_path / "docs"
    docs_root.mkdir()
    file_path = docs_root / "a.md"
    file_path.write_text("# Test", encoding="utf-8")

    monkeypatch.setattr(module, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(module, "UnifiedLogger", _DummyUnifiedLogger())

    result = module.write_inventory(
        inventory_path=inventory_path,
        hashes_path=hashes_path,
        files=(file_path,),
    )

    assert inventory_path.exists()
    assert hashes_path.exists()
    assert result.files == (file_path,)

