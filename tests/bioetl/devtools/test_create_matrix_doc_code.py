from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from bioetl.devtools import cli_create_matrix_doc_code as create_matrix_doc_code


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


def test_build_and_write_matrix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rows = create_matrix_doc_code.build_matrix()
    assert rows

    monkeypatch.setattr(create_matrix_doc_code, "UnifiedLogger", DummyUnifiedLogger)
    result = create_matrix_doc_code.write_matrix(artifacts_dir=tmp_path)
    assert result.csv_path.exists()
    assert result.json_path.exists()
    data = json.loads(result.json_path.read_text(encoding="utf-8"))
    assert data[0]["doc_point"]

