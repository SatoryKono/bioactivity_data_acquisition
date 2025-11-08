"""Тесты для semantic_diff."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any
from types import SimpleNamespace

import pytest

from bioetl.tools import semantic_diff as module


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

    def configure(self) -> None:
        return None

    def get(self, _: str) -> _DummyLogger:
        return self.logger


@pytest.fixture(autouse=True)
def patch_logger(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "UnifiedLogger", _DummyUnifiedLogger())


def test_extract_method_signature_from_code_handles_defaults() -> None:
    def sample(a: int, b: str = "value") -> bool:
        return True

    signature = module.extract_method_signature_from_code(sample)
    assert signature["name"] == "sample"
    assert signature["parameters"][1]["default"] == "value"


def test_compare_methods_detects_gaps() -> None:
    code = {"run": {"parameters": [], "return_annotation": "int"}}
    docs = {}
    differences = module.compare_methods(code, docs)
    assert differences["run"]["status"] == "gap"


def test_run_semantic_diff_writes_report(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module.ARTIFACTS_DIR = tmp_path

    monkeypatch.setattr(
        module,
        "extract_pipeline_base_methods",
        lambda: {"run": {"parameters": [], "return_annotation": "int"}},
    )
    monkeypatch.setattr(
        module,
        "extract_pipeline_base_from_docs",
        lambda: {"run": {"parameters": [], "return_annotation": "int"}},
    )
    monkeypatch.setattr(
        module,
        "extract_config_fields_from_code",
        lambda: {"field": {"type": "str"}},
    )
    monkeypatch.setattr(
        module,
        "extract_config_fields_from_docs",
        lambda: {"field": {"type": "str"}},
    )
    monkeypatch.setattr(
        module,
        "extract_cli_flags_from_code",
        lambda: [{"name": "--flag", "required": False}],
    )
    monkeypatch.setattr(
        module,
        "extract_cli_flags_from_docs",
        lambda: [{"name": "--flag", "required": False}],
    )

    report_path = module.run_semantic_diff()

    assert report_path.exists()
    data = json.loads(report_path.read_text(encoding="utf-8"))
    assert data["methods"]["run"]["status"] == "ok"


def test_extract_pipeline_base_from_docs_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "DOCS_ROOT", tmp_path)
    result = module.extract_pipeline_base_from_docs()
    assert "error" in result


def test_extract_config_fields_from_code(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Field:
        def __init__(self, annotation: str, default: Any = None) -> None:
            self.annotation = annotation
            self.default = default

        def is_required(self) -> bool:
            return self.default is None

    class _PipelineConfig:
        model_fields = {
            "name": _Field("str"),
            "timeout": _Field("int", default=5),
        }

    fake_module = SimpleNamespace(PipelineConfig=_PipelineConfig)
    monkeypatch.setitem(sys.modules, "bioetl.config.models.base", fake_module)

    fields = module.extract_config_fields_from_code()
    assert fields["name"]["required"] is True
    assert fields["timeout"]["default"] == "5"


def test_extract_config_fields_from_docs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    doc_root = tmp_path
    module.DOCS_ROOT = doc_root
    file_path = doc_root / "configs" / "00-typed-configs-and-profiles.md"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(
        "| `name` | string | yes | default | description |\n",
        encoding="utf-8",
    )

    fields = module.extract_config_fields_from_docs()
    assert fields["name"]["required"] is True
    assert fields["name"]["default"] == "default"


def test_compare_methods_detects_contradiction() -> None:
    code = {"run": {"parameters": [{"name": "a"}], "return_annotation": "int"}}
    docs = {"run": {"parameters": [], "return_annotation": "str"}}
    differences = module.compare_methods(code, docs)
    assert differences["run"]["status"] == "contradiction"


def test_extract_pipeline_base_methods_uses_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    class _PipelineBase:
        def extract(self, a: int) -> int:  # pragma: no cover - simple stub
            return a

        def transform(self, df: Any) -> Any:  # pragma: no cover - simple stub
            return df

    fake_module = SimpleNamespace(PipelineBase=_PipelineBase)
    monkeypatch.setitem(sys.modules, "bioetl.pipelines.base", fake_module)

    methods = module.extract_pipeline_base_methods()
    assert "extract" in methods


def test_extract_pipeline_base_from_docs_parses_content(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    doc_root = tmp_path
    module.DOCS_ROOT = doc_root
    file_path = doc_root / "pipelines" / "00-pipeline-base.md"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(
        "def run(self, output_dir: Path) -> RunResult:\n    pass\n",
        encoding="utf-8",
    )

    methods = module.extract_pipeline_base_from_docs()
    assert methods["run"]["return_annotation"] == "RunResult"


def test_extract_cli_flags_from_docs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    doc_root = tmp_path
    module.DOCS_ROOT = doc_root
    file_path = doc_root / "cli" / "01-cli-commands.md"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(
        "| `--config` | `-c` | yes | Path to config |\n",
        encoding="utf-8",
    )

    flags = module.extract_cli_flags_from_docs()
    assert flags[0]["name"] == "--config"
    assert flags[0]["required"] is True

