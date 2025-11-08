"""Тесты для качественных утилит (`semantic_diff`, `check_output_artifacts`)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import bioetl.tools.check_output_artifacts as check_output_artifacts
import bioetl.tools.semantic_diff as semantic_diff


@pytest.mark.unit
def test_extract_pipeline_base_from_docs_parses_methods(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Регулярное выражение корректно извлекает сигнатуры методов из документа."""

    monkeypatch.setattr(semantic_diff, "DOCS_ROOT", tmp_path)
    doc_path = tmp_path / "pipelines" / "00-pipeline-base.md"
    doc_path.parent.mkdir(parents=True)
    doc_path.write_text(
        "def extract(self, limit: int) -> Iterable:\n"
        "    pass\n"
        "def transform(self, data) -> None:\n"
        "    pass\n",
        encoding="utf-8",
    )

    methods = semantic_diff.extract_pipeline_base_from_docs()

    assert "extract" in methods
    assert methods["extract"]["parameters"][0]["name"] == "self"
    assert methods["transform"]["return_annotation"] == "None"


@pytest.mark.unit
def test_extract_config_fields_from_docs_missing_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """При отсутствии документа возвращается диагностическое сообщение."""

    monkeypatch.setattr(semantic_diff, "DOCS_ROOT", tmp_path)

    fields = semantic_diff.extract_config_fields_from_docs()

    assert "error" in fields
    assert "Documentation file not found" in fields["error"]


@pytest.mark.unit
def test_compare_methods_reports_statuses() -> None:
    """Проверяем все ветви: совпадение, противоречие, разрыв."""

    code_methods = {
        "run": {"return_annotation": "None", "parameters": []},
        "extract": {"return_annotation": "DataFrame", "parameters": [{"name": "self"}]},
    }
    doc_methods = {
        "run": {"return_annotation": "None", "parameters": []},
        "transform": {"return_annotation": "None", "parameters": []},
        "extract": {"return_annotation": "Dataset", "parameters": []},
    }

    differences = semantic_diff.compare_methods(code_methods, doc_methods)

    assert differences["run"]["status"] == "ok"
    assert differences["transform"]["status"] == "gap"
    assert differences["extract"]["status"] == "contradiction"


@pytest.mark.unit
def test_run_semantic_diff_generates_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    patch_unified_logger,
    track_path_replace,
) -> None:
    """Отчёт формируется атомарно с ожидаемым содержимым."""

    monkeypatch.setattr(semantic_diff, "ARTIFACTS_DIR", tmp_path / "artifacts")
    patch_unified_logger(semantic_diff)
    monkeypatch.setattr(semantic_diff, "extract_pipeline_base_methods", lambda: {"run": {"return_annotation": "None", "parameters": []}})
    monkeypatch.setattr(semantic_diff, "extract_pipeline_base_from_docs", lambda: {"run": {"return_annotation": "None", "parameters": []}})
    monkeypatch.setattr(semantic_diff, "extract_config_fields_from_code", lambda: {"field": {"type": "int"}})
    monkeypatch.setattr(semantic_diff, "extract_config_fields_from_docs", lambda: {"field": {"type": "int"}})
    monkeypatch.setattr(semantic_diff, "extract_cli_flags_from_code", lambda: [{"name": "--flag"}])
    monkeypatch.setattr(semantic_diff, "extract_cli_flags_from_docs", lambda: [{"name": "--flag"}])

    report_path = semantic_diff.run_semantic_diff()

    assert report_path.exists()
    assert track_path_replace, "ожидался вызов Path.replace для атомарной записи"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["methods"]["run"]["status"] == "ok"


@pytest.mark.unit
def test_check_output_artifacts_detects_issues(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    patch_unified_logger,
) -> None:
    """Проверяем, что утилита корректно собирает ошибки по трекаемым, застейдженным и крупным файлам."""

    repo_root = tmp_path / "repo"
    output_dir = repo_root / "data" / "output"
    output_dir.mkdir(parents=True)
    big_file = output_dir / "big.parquet"
    big_file.write_bytes(b"x" * 20)

    monkeypatch.setattr(check_output_artifacts, "get_project_root", lambda: repo_root)
    patch_unified_logger(check_output_artifacts)
    monkeypatch.setattr(check_output_artifacts, "_git_ls_files", lambda path: [Path("data/output/tracked.csv")])
    monkeypatch.setattr(check_output_artifacts, "_git_diff_cached", lambda path: [Path("data/output/new.csv")])

    errors = check_output_artifacts.check_output_artifacts(max_bytes=10)

    assert len(errors) == 3
    assert "Tracked artifacts detected" in errors[0]
    assert "New artifacts staged" in errors[1]
    assert "Large files found" in errors[2]

