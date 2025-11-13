"""Дополнительные тесты для catalog_code_symbols."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from bioetl.tools import catalog_code_symbols as module


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

    def configure(self) -> None:  # pragma: no cover - simple stub
        return None

    def get(self, _: str) -> _DummyLogger:
        return self.logger


def test_extract_method_signature_includes_defaults() -> None:
    def sample(a: int, b: str = "value") -> bool:
        return True

    signature = module.extract_method_signature(sample)

    assert signature["name"] == "sample"
    assert signature["return_annotation"] == "bool"
    defaults = {item["name"]: item["default"] for item in signature["parameters"]}
    assert defaults["b"] == "value"


def test_catalog_code_symbols_writes_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dummy_unified = _DummyUnifiedLogger()
    monkeypatch.setattr(module, "UnifiedLogger", dummy_unified)

    result = module.catalog_code_symbols(artifacts_dir=tmp_path)

    json_path = tmp_path / "code_signatures.json"
    cli_path = tmp_path / "cli_commands.txt"

    assert json_path.exists()
    assert cli_path.exists()
    assert result.json_path == json_path
    assert result.cli_path == cli_path

