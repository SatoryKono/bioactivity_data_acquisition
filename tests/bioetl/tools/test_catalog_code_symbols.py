from __future__ import annotations

import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from bioetl.tools import catalog_code_symbols


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


def _install_dummy_modules(monkeypatch: pytest.MonkeyPatch) -> None:
    base_module = ModuleType("bioetl.pipelines.base")

    class DummyPipelineBase:
        def extract(self, value: int) -> int:
            return value

        def run(self, path: Path) -> str:
            return str(path)

    base_module.PipelineBase = DummyPipelineBase
    monkeypatch.setitem(sys.modules, "bioetl.pipelines.base", base_module)

    config_module = ModuleType("bioetl.config.models")

    class DummyField(SimpleNamespace):
        def is_required(self) -> bool:
            return getattr(self, "_required", False)

    class DummyModel:
        model_fields = {
            "name": DummyField(annotation=str, default="demo", _required=False),
        }

    config_module.PipelineConfig = DummyModel
    config_module.PipelineMetadata = DummyModel
    config_module.DeterminismConfig = DummyModel
    monkeypatch.setitem(sys.modules, "bioetl.config.models", config_module)

    cli_module = ModuleType("bioetl.cli.cli_registry")
    cli_module.COMMAND_REGISTRY = {"activity": object(), "assay": object()}
    monkeypatch.setitem(sys.modules, "bioetl.cli.cli_registry", cli_module)


def test_catalog_code_symbols(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _install_dummy_modules(monkeypatch)
    monkeypatch.setattr(catalog_code_symbols, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(catalog_code_symbols, "PROJECT_ROOT", tmp_path)
    artifacts = tmp_path / "artifacts"
    result = catalog_code_symbols.catalog_code_symbols(artifacts_dir=artifacts)
    assert result.json_path.exists()
    assert result.cli_path.exists()
    payload = json.loads(result.json_path.read_text(encoding="utf-8"))
    assert "pipeline_base" in payload

