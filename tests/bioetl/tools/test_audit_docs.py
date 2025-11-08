"""Тесты для аудита документации."""

from __future__ import annotations

import csv
from collections.abc import Callable
from contextlib import nullcontext
from pathlib import Path
from typing import Any

import pytest

from bioetl.tools import audit_docs


class _LoggerStub:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []
        self.warnings: list[tuple[str, dict[str, Any]]] = []
        self.errors: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: str, **context: Any) -> None:
        self.events.append((event, context))

    def warning(self, event: str, **context: Any) -> None:
        self.warnings.append((event, context))

    def error(self, event: str, **context: Any) -> None:
        self.errors.append((event, context))


@pytest.fixture
def docs_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[_LoggerStub, Path]:
    """Готовим изолированную среду и подменяем UnifiedLogger."""

    logger = _LoggerStub()
    docs_root = tmp_path / "docs"
    docs_root.mkdir()

    monkeypatch.setattr("bioetl.tools.audit_docs.ROOT", tmp_path, raising=False)
    monkeypatch.setattr("bioetl.tools.audit_docs.DOCS", docs_root, raising=False)
    monkeypatch.setattr("bioetl.tools.audit_docs.UnifiedLogger.configure", lambda: None)
    monkeypatch.setattr("bioetl.tools.audit_docs.UnifiedLogger.bind", lambda **_: None)
    monkeypatch.setattr("bioetl.tools.audit_docs.UnifiedLogger.get", lambda *_: logger)
    monkeypatch.setattr("bioetl.tools.audit_docs.UnifiedLogger.stage", lambda *args, **kwargs: nullcontext())

    return logger, docs_root


@pytest.mark.unit
def test_extract_markdown_links_sorted() -> None:
    """Проверяем сортировку и извлечение ссылок."""

    content = "[B](b.md)\n[A](a.md)\n[External](https://example.com)"
    links = audit_docs.extract_markdown_links(content)

    assert links == [("A", "a.md"), ("B", "b.md"), ("External", "https://example.com")]


@pytest.mark.unit
def test_check_file_exists_prefers_relative(tmp_path: Path) -> None:
    """Относительные пути разрешаются относительно базового каталога."""

    base = tmp_path / "docs"
    base.mkdir()
    target = base / "chapter" / "file.md"
    target.parent.mkdir()
    target.write_text("content", encoding="utf-8")

    exists, resolved = audit_docs.check_file_exists("chapter/file.md", base)

    assert exists is True
    assert resolved == target


@pytest.mark.unit
def test_audit_broken_links_detects_missing(
    docs_env: tuple[_LoggerStub, Path], tmp_path: Path
) -> None:
    """Находим битую ссылку в markdown-документе."""

    _, docs_root = docs_env
    doc = docs_root / "guide.md"
    doc.write_text("[Valid](existing.md)\n[Broken](missing.md)", encoding="utf-8")
    (doc.parent / "existing.md").write_text("ok", encoding="utf-8")

    broken = audit_docs.audit_broken_links()

    assert len(broken) == 1
    record = broken[0]
    assert Path(record["source"]) == Path("docs/guide.md")
    assert record["link_text"] == "Broken"
    assert record["link_path"] == "missing.md"
    assert record["type"] == "broken_internal_link"


@pytest.mark.unit
def test_audit_broken_links_missing_directory(monkeypatch: pytest.MonkeyPatch) -> None:
    """Логируем предупреждение если каталога docs нет."""

    logger = _LoggerStub()
    monkeypatch.setattr("bioetl.tools.audit_docs.DOCS", Path("/tmp/nonexistent"), raising=False)
    monkeypatch.setattr("bioetl.tools.audit_docs.ROOT", Path("/tmp"), raising=False)
    monkeypatch.setattr("bioetl.tools.audit_docs.UnifiedLogger.get", lambda *_: logger)

    broken = audit_docs.audit_broken_links()

    assert broken == []
    assert logger.warnings and logger.warnings[0][0] == "docs_directory_missing"


@pytest.mark.unit
def test_find_lychee_missing_reports(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Отчёт о файлах, заявленных в lychee, но отсутствующих."""

    monkeypatch.setattr("bioetl.tools.audit_docs.ROOT", tmp_path, raising=False)
    monkeypatch.setattr(
        "bioetl.tools.audit_docs.LYCHEE_MISSING",
        ["docs/a.md", "docs/b.md"],
        raising=False,
    )

    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "a.md").write_text("ok", encoding="utf-8")

    missing = audit_docs.find_lychee_missing()

    assert missing == [
        {"source": ".lychee.toml", "file": "docs/b.md", "type": "declared_but_missing"}
    ]


@pytest.mark.unit
def test_extract_pipeline_info_detects_sections(docs_env: tuple[_LoggerStub, Path]) -> None:
    """Проверяем определение признаков в документации по пайплайну."""

    _, docs_root = docs_env
    doc = docs_root / "activity-chembl-extraction.md"
    doc.write_text(
        """
Usage CLI command
Configuration via YAML profiles
Schema validation with Pandera
Input CSV and output parquet
Determinism hash_row info
QC golden files
Structured logging JSON
""",
        encoding="utf-8",
    )

    info = audit_docs.extract_pipeline_info("activity")

    assert Path(info["doc_path"]) == Path("docs/activity-chembl-extraction.md")
    assert info["has_cli"] is True
    assert info["has_config"] is True
    assert info["has_schema"] is True
    assert info["has_io"] is True
    assert info["has_determinism"] is True
    assert info["has_qc"] is True
    assert info["has_logging"] is True


@pytest.mark.unit
def test_write_csv_and_markdown_atomic(tmp_path: Path) -> None:
    """Проверяем атомарную запись CSV и Markdown."""

    csv_path = tmp_path / "out" / "table.csv"
    markdown_path = tmp_path / "out" / "report.md"

    audit_docs._write_csv_atomic(
        csv_path,
        fieldnames=["col1", "col2"],
        rows=[{"col1": "a", "col2": "b"}],
    )
    audit_docs._write_markdown_atomic(markdown_path, ["# Header", "Line"])

    with csv_path.open(encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == ["col1", "col2"]
        rows = list(reader)
        assert rows == [{"col1": "a", "col2": "b"}]

    content = markdown_path.read_text(encoding="utf-8")
    assert content.endswith("\n")
    assert "# Header" in content


@pytest.mark.unit
def test_run_audit_emits_artifacts(
    docs_env: tuple[_LoggerStub, Path], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Упрощённый сценарий запуска аудита с заглушками зависимостей."""

    logger, _ = docs_env
    artifacts_dir = tmp_path / "artifacts"

    broken_links = [
        {
            "source": "docs/example.md",
            "link_text": "Broken",
            "link_path": "missing.md",
            "type": "broken_internal_link",
        }
    ]
    lychee_missing = [
        {"source": ".lychee.toml", "file": "docs/missing.md", "type": "declared_but_missing"}
    ]
    pipeline_stub = {
        "pipeline": "activity",
        "doc_path": "docs/activity.md",
        "has_cli": True,
        "has_config": False,
        "has_schema": True,
        "has_io": False,
        "has_determinism": True,
        "has_qc": False,
        "has_logging": True,
    }

    monkeypatch.setattr("bioetl.tools.audit_docs.audit_broken_links", lambda: broken_links)
    monkeypatch.setattr("bioetl.tools.audit_docs.find_lychee_missing", lambda: lychee_missing)
    monkeypatch.setattr("bioetl.tools.audit_docs.ALL_PIPELINES", ["activity"], raising=False)
    monkeypatch.setattr("bioetl.tools.audit_docs.extract_pipeline_info", lambda *_: dict(pipeline_stub))

    csv_calls: dict[str, Any] = {}
    markdown_calls: dict[str, Any] = {}

    def capture_csv(path: Path, fieldnames, rows) -> None:
        csv_calls["path"] = path
        csv_calls["fieldnames"] = list(fieldnames)
        csv_calls["rows"] = list(rows)

    def capture_markdown(path: Path, lines) -> None:
        markdown_calls["path"] = path
        markdown_calls["lines"] = list(lines)

    monkeypatch.setattr("bioetl.tools.audit_docs._write_csv_atomic", capture_csv)
    monkeypatch.setattr("bioetl.tools.audit_docs._write_markdown_atomic", capture_markdown)

    audit_docs.run_audit(artifacts_dir=artifacts_dir)

    assert csv_calls["path"] == artifacts_dir / "GAPS_TABLE.csv"
    assert markdown_calls["path"] == artifacts_dir / "LINKCHECK.md"
    assert any(event[0] == "audit_finished" for event in logger.events)

