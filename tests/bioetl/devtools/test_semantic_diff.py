from __future__ import annotations

import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from bioetl.devtools import cli_semantic_diff  # type: ignore[reportMissingImports]
from bioetl.devtools.cli_semantic_diff import (  # type: ignore[reportMissingImports]
    compare_methods,
    extract_cli_flags_from_code,
    extract_cli_flags_from_docs,
    extract_config_fields_from_code,
    extract_config_fields_from_docs,
    extract_pipeline_base_from_docs,
    extract_pipeline_base_methods,
    run_semantic_diff,
)


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
    module = ModuleType("bioetl.core.pipeline")

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

    setattr(module, "PipelineBase", DummyPipelineBase)
    monkeypatch.setitem(sys.modules, "bioetl.core.pipeline", module)


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

    setattr(module, "PipelineConfig", DummyModel)
    setattr(module, "PipelineMetadata", DummyModel)
    setattr(module, "DeterminismConfig", DummyModel)
    monkeypatch.setitem(sys.modules, "bioetl.config.models", module)


def test_extractors_and_compare_methods(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_dummy_pipeline_base(monkeypatch)
    _install_dummy_config_models(monkeypatch)
    monkeypatch.setattr(cli_semantic_diff, "UnifiedLogger", DummyUnifiedLogger)

    project_root = tmp_path
    docs_root = project_root / "docs"
    docs_root.mkdir(parents=True)
    (docs_root / "pipelines").mkdir(exist_ok=True)
    (docs_root / "configs").mkdir(exist_ok=True)
    (docs_root / "cli").mkdir(exist_ok=True)

    (docs_root / "pipelines" / "00-pipeline-base.md").write_text(
        """# PipelineBase

```python
class PipelineBase:
    def __init__(self, config, run_id: str):
        ...

    def extract(self, value: int) -> pd.DataFrame:
        ...

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        ...

    def validate(self, data: pd.DataFrame) -> pd.DataFrame:
        ...

    def write(self, data: pd.DataFrame, output_path: Path) -> RunResult:
        ...

    def run(self, output_path: Path) -> RunResult:
        ...
```
""",
        encoding="utf-8",
    )

    (docs_root / "configs" / "00-typed-configs-and-profiles.md").write_text(
        """
        | `version` | int | yes | 1 | Schema version |
        | `pipeline` | object | yes | — | Pipeline metadata |
        | `domain` | object | no | {} | Domain config |
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

    # Создаём мок-файл cli_command.py для извлечения CLI флагов
    src_dir = project_root / "src" / "bioetl" / "cli"
    src_dir.mkdir(parents=True)
    (src_dir / "cli_command.py").write_text(
        """
import typer
from pathlib import Path

def create_pipeline_command(pipeline_class, command_config):
    def command(
        config: Path = typer.Option(..., "--config", "-c", help="Path to configuration file"),
        output_dir: Path = typer.Option(Path("data/output"), "--output-dir", "-o", help="Output directory"),
        dry_run: bool = typer.Option(False, "--dry-run", help="Dry run mode"),
    ) -> None:
        pass
    return command
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(cli_semantic_diff, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(cli_semantic_diff, "DOCS_ROOT", docs_root)
    artifacts = project_root / "artifacts"
    monkeypatch.setattr(cli_semantic_diff, "ARTIFACTS_DIR", artifacts)

    method_info = extract_pipeline_base_methods()
    assert "extract" in method_info

    doc_methods = extract_pipeline_base_from_docs()
    assert "extract" in doc_methods

    differences = compare_methods(method_info, doc_methods)
    assert "extract" in differences

    config_fields_code = extract_config_fields_from_code()
    # Проверяем, что функция возвращает словарь с полями (не ошибку)
    assert isinstance(config_fields_code, dict)
    assert "error" not in config_fields_code
    # Проверяем наличие хотя бы одного реального поля PipelineConfig
    assert "pipeline" in config_fields_code or "version" in config_fields_code

    config_fields_docs = extract_config_fields_from_docs()
    # Проверяем, что функция возвращает словарь с полями (не ошибку)
    assert isinstance(config_fields_docs, dict)
    assert "error" not in config_fields_docs
    # Проверяем, что есть хотя бы одно поле
    assert len(config_fields_docs) > 0

    cli_flags_code = extract_cli_flags_from_code()
    # Проверяем, что функция возвращает список флагов (не ошибку)
    assert isinstance(cli_flags_code, list)
    assert len(cli_flags_code) > 0
    assert "error" not in cli_flags_code[0] if cli_flags_code else True
    assert any(flag.get("name") == "--config" for flag in cli_flags_code if isinstance(flag, dict))

    cli_flags_docs = extract_cli_flags_from_docs()
    assert cli_flags_docs[0]["required"] is True

    report_path = run_semantic_diff()
    assert report_path.exists()
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert "methods" in payload

