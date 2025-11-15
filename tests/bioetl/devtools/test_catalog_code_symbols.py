from __future__ import annotations

import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from bioetl.devtools import cli_catalog_code_symbols as catalog_code_symbols


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
    base_module = ModuleType("bioetl.core.pipeline")

    class DummyPipelineBase:
        def extract(self, value: int) -> int:
            return value

        def run(self, path: Path) -> str:
            return str(path)

    base_module.PipelineBase = DummyPipelineBase
    monkeypatch.setitem(sys.modules, "bioetl.core.pipeline", base_module)

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
    cli_module.PIPELINE_REGISTRY = (
        SimpleNamespace(code="activity"),
        SimpleNamespace(code="assay"),
    )
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


def test_helper_functions(tmp_path: Path) -> None:
    target_dir = tmp_path / "nested"
    catalog_code_symbols._ensure_dir(target_dir)
    assert target_dir.exists()

    text_path = tmp_path / "sample.txt"
    catalog_code_symbols._write_text_atomic(text_path, "payload")
    assert text_path.read_text(encoding="utf-8") == "payload"

    json_path = tmp_path / "sample.json"
    catalog_code_symbols._write_json_atomic(json_path, {"a": 1})
    assert json.loads(json_path.read_text(encoding="utf-8")) == {"a": 1}


def test_extract_method_signature_details() -> None:
    def sample_method(arg: int, flag: bool = False) -> str:
        return f"{arg}-{flag}"

    signature = catalog_code_symbols.extract_method_signature(sample_method)
    assert signature["name"] == "sample_method"
    assert any(param["name"] == "arg" for param in signature["parameters"])
    assert signature["return_annotation"] == "str"


def test_extract_pipeline_and_config_models(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_dummy_modules(monkeypatch)
    signatures = catalog_code_symbols.extract_pipeline_base_signatures()
    assert "extract" in signatures
    config = catalog_code_symbols.extract_config_models()
    assert "PipelineConfig" in config
    assert "fields" in config["PipelineConfig"]


def test_extract_cli_commands_returns_sorted(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_dummy_modules(monkeypatch)
    commands = catalog_code_symbols.extract_cli_commands()
    assert commands == ["activity", "assay"]