from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterator

import pytest

from bioetl.tools import audit_docs


class DummyLogger:
    def info(self, *args: Any, **kwargs: Any) -> None:
        pass

    def warning(self, *args: Any, **kwargs: Any) -> None:
        pass

    def error(self, *args: Any, **kwargs: Any) -> None:
        pass


class DummyUnifiedLogger:
    @staticmethod
    def configure() -> None:
        pass

    @staticmethod
    def bind(**_: Any) -> None:
        pass

    @staticmethod
    def get(_: str) -> DummyLogger:
        return DummyLogger()

    @staticmethod
    def stage(name: str) -> Iterator[None]:
        class _Stage:
            def __enter__(self) -> None:
                return None

            def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
                return None

        return _Stage()


def test_extract_markdown_links_and_check(tmp_path: Path) -> None:
    doc = tmp_path / "doc.md"
    referenced = tmp_path / "ref.md"
    referenced.write_text("ref", encoding="utf-8")
    doc.write_text("[ok](ref.md)\n[missing](missing.md)", encoding="utf-8")

    links = audit_docs.extract_markdown_links(doc.read_text(encoding="utf-8"))
    assert links == [("missing", "missing.md"), ("ok", "ref.md")]
    exists, path = audit_docs.check_file_exists("ref.md", doc.parent)
    assert exists and path == referenced
    exists, _ = audit_docs.check_file_exists("missing.md", doc.parent)
    assert not exists


def test_audit_broken_links_and_find_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    valid = docs_dir / "file.md"
    valid.write_text("[broken](missing.md)", encoding="utf-8")

    monkeypatch.setattr(audit_docs, "ROOT", tmp_path)
    monkeypatch.setattr(audit_docs, "DOCS", docs_dir)
    monkeypatch.setattr(audit_docs, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(audit_docs, "LYCHEE_MISSING", ["docs/file.md", "docs/absent.md"])

    broken = audit_docs.audit_broken_links()
    assert broken[0]["link_path"] == "missing.md"

    missing = audit_docs.find_lychee_missing()
    assert any(item["file"] == "docs/absent.md" for item in missing)


def test_extract_pipeline_info(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    docs_dir = tmp_path / "docs"
    pipeline_dir = docs_dir / "pipelines"
    pipeline_dir.mkdir(parents=True)
    doc_path = pipeline_dir / "activity-extraction.md"
    doc_path.write_text(
        "CLI usage\nconfig yaml\nschema details\ninput output\nhash_row\nqc metrics\nlogging\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(audit_docs, "ROOT", tmp_path)
    monkeypatch.setattr(audit_docs, "DOCS", docs_dir)
    info = audit_docs.extract_pipeline_info("activity")
    assert info["has_cli"]
    assert info["has_logging"]


def test_atomic_writers(tmp_path: Path) -> None:
    csv_path = tmp_path / "out.csv"
    md_path = tmp_path / "out.md"
    audit_docs._write_csv_atomic(csv_path, ["a"], [{"a": "1"}])
    audit_docs._write_markdown_atomic(md_path, ["line"])

    with csv_path.open(encoding="utf-8") as handle:
        reader = csv.reader(handle)
        rows = list(reader)
    assert rows[0][0] == "a"
    assert md_path.read_text(encoding="utf-8").startswith("line")


def test_run_audit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    docs_dir.joinpath("sample.md").write_text("[ref](missing.md)", encoding="utf-8")

    artifacts = tmp_path / "artifacts"
    monkeypatch.setattr(audit_docs, "ROOT", tmp_path)
    monkeypatch.setattr(audit_docs, "DOCS", docs_dir)
    monkeypatch.setattr(audit_docs, "UnifiedLogger", DummyUnifiedLogger)

    audit_docs.run_audit(artifacts_dir=artifacts)
    assert (artifacts / "GAPS_TABLE.csv").exists()
    assert (artifacts / "LINKCHECK.md").exists()

