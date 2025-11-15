from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest

from bioetl.cli.tools._logic import cli_inventory_docs as inventory_docs


class DummyLogger:
    def info(self, *args: Any, **kwargs: Any) -> None:
        pass


class DummyUnifiedLogger:
    @staticmethod
    def configure() -> None:
        pass

    @staticmethod
    def get(_: str) -> DummyLogger:
        return DummyLogger()


def test_collect_markdown_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    docs_root = tmp_path / "docs"
    docs_root.mkdir()
    files = [docs_root / f"{name}.md" for name in ("a", "b")]
    for path in files:
        path.write_text("content", encoding="utf-8")

    monkeypatch.setattr(inventory_docs, "get_project_root", lambda: tmp_path)
    collected = inventory_docs.collect_markdown_files(docs_root=docs_root)
    assert collected == tuple(sorted(files))


def test_write_inventory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    docs_root = tmp_path / "docs"
    docs_root.mkdir()
    doc = docs_root / "a.md"
    doc.write_text("content", encoding="utf-8")

    monkeypatch.setattr(inventory_docs, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(inventory_docs, "get_project_root", lambda: tmp_path)

    result = inventory_docs.write_inventory(
        tmp_path / "inventory.txt",
        tmp_path / "hashes.txt",
        files=(doc,),
    )
    assert result.inventory_path.exists()
    assert result.hashes_path.exists()
    assert "docs/a.md" in result.inventory_path.read_text(encoding="utf-8")

    hashes_lines = result.hashes_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(hashes_lines) == 1
    digest, recorded_path = hashes_lines[0].split(maxsplit=1)
    assert recorded_path == "docs/a.md"
    assert digest == hashlib.sha256(doc.read_bytes()).hexdigest()

