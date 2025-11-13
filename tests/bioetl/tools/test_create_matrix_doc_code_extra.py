"""Дополнительные тесты для create_matrix_doc_code."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from bioetl.tools import create_matrix_doc_code as module


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

    def configure(self) -> None:  # pragma: no cover - простая заглушка
        return None

    def get(self, _: str) -> _DummyLogger:
        return self.logger


def test_build_matrix_contains_expected_contracts() -> None:
    matrix = module.build_matrix()
    doc_points = {row["doc_point"] for row in matrix}

    assert "transform signature: transform(df: pd.DataFrame) -> pd.DataFrame" in doc_points
    assert "CLI command: activity_chembl" in doc_points


def test_write_matrix_creates_atomic_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "UnifiedLogger", _DummyUnifiedLogger())
    result = module.write_matrix(artifacts_dir=tmp_path)

    assert result.csv_path.exists()
    assert result.json_path.exists()
    csv_content = result.csv_path.read_text(encoding="utf-8")
    json_content = result.json_path.read_text(encoding="utf-8")

    assert "doc_point" in csv_content
    assert "[" in json_content

