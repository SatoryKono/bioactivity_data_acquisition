from __future__ import annotations

import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from bioetl.cli.tools._logic import cli_semantic_diff as semantic_diff


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


def _install_dummy_pipeline_base(monkeypatch: pytest.MonkeyPatch) -> None:
    module = ModuleType("bioetl.pipelines.base")

    class DummyPipelineBase:
        def extract(self, value: int, *, flag: bool = True) -> int:
            return value

        def transform(self, data: list[int]) -> list[int]:
            return data

        def validate(self, data: list[int]) -> list[int]:
            return data

        def write(self, data: list[int], path: Path, extended: bool = False) -> str:
            return str(path)

        def run(self, path: Path, extended: bool = False) -> str:
            return str(path)

    module.PipelineBase = DummyPipelineBase
    monkeypatch.setitem(sys.modules, "bioetl.pipelines.base", module)


def _install_dummy_config_models(monkeypatch: pytest.MonkeyPatch) -> None:
    module = ModuleType("bioetl.config.models")

    class DummyField(SimpleNamespace):
        def is_required(self) -> bool:
            return getattr(self, "_required", False)

    class DummyModel:
        model_fields = {
            "name": DummyField(annotation=str, default="demo", _required=False),
            "count": DummyField(annotation=int, default=None, _required=True),
        }

    module.PipelineConfig = DummyModel
    module.PipelineMetadata = DummyModel
    module.DeterminismConfig = DummyModel
    monkeypatch.setitem(sys.modules, "bioetl.config.models", module)


def test_extractors_and_compare_methods(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_dummy_pipeline_base(monkeypatch)
    _install_dummy_config_models(monkeypatch)
    monkeypatch.setattr(semantic_diff, "UnifiedLogger", DummyUnifiedLogger)

    project_root = tmp_path
    docs_root = project_root / "docs"
    docs_root.mkdir(parents=True)
    (docs_root / "pipelines").mkdir(exist_ok=True)
    (docs_root / "configs").mkdir(exist_ok=True)
    (docs_root / "cli").mkdir(exist_ok=True)

    (docs_root / "pipelines" / "00-pipeline-base.md").write_text(
        """
        ```python
        def extract(value: int) -> pd.DataFrame:
            ...
        def transform(data) -> pd.DataFrame:
            ...
        ```
        """,
        encoding="utf-8",
    )

    (docs_root / "configs" / "00-typed-configs-and-profiles.md").write_text(
        """
        | `name` | string | yes | demo | entry |
        | `count` | int | no | — | amount |
        """,
        encoding="utf-8",
    )

    (docs_root / "cli" / "01-cli-commands.md").write_text(
        """
        | `--config` | `-c` | yes | Provide config |
        | `--dry-run` | `-n` | no | Dry mode |
        """,
        encoding="utf-8",
    )

    monkeypatch.setattr(semantic_diff, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(semantic_diff, "DOCS_ROOT", docs_root)
    artifacts = project_root / "artifacts"
    monkeypatch.setattr(semantic_diff, "ARTIFACTS_DIR", artifacts)

    method_info = semantic_diff.extract_pipeline_base_methods()
    assert "extract" in method_info

    doc_methods = semantic_diff.extract_pipeline_base_from_docs()
    assert "extract" in doc_methods

    differences = semantic_diff.compare_methods(method_info, doc_methods)
    assert "extract" in differences

    config_fields_code = semantic_diff.extract_config_fields_from_code()
    assert "name" in config_fields_code

    config_fields_docs = semantic_diff.extract_config_fields_from_docs()
    assert config_fields_docs["name"]["required"] is True

    cli_flags_code = semantic_diff.extract_cli_flags_from_code()
    assert any(flag["name"] == "--config" for flag in cli_flags_code)

    cli_flags_docs = semantic_diff.extract_cli_flags_from_docs()
    assert cli_flags_docs[0]["required"] is True

    report_path = semantic_diff.run_semantic_diff()
    assert report_path.exists()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert "methods" in payload

