from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator

import pytest

from bioetl.tools import vocab_audit


class DummyLogger:
    def info(self, *args: Any, **kwargs: Any) -> None:
        pass

    def warning(self, *args: Any, **kwargs: Any) -> None:
        pass


class DummyUnifiedLogger:
    @staticmethod
    def configure() -> None:
        pass

    @staticmethod
    def get(_: str) -> DummyLogger:
        return DummyLogger()


class DummyQuery:
    def __init__(self, records: list[dict[str, Any]]) -> None:
        self._records = records

    def only(self, _: str) -> DummyQuery:
        return self

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self._records)


class DummyResource:
    def __init__(self, records: list[dict[str, Any]]) -> None:
        self._records = records

    def filter(self, **_: Any) -> DummyQuery:
        return DummyQuery(self._records)


class DummyClient:
    def __init__(self) -> None:
        self.activity = DummyResource(
            [
                {"standard_type": "IC50"},
                {"standard_type": "EC50"},
            ]
        )
        self.assay = DummyResource([{"assay_type": "binding"}])


def test_dictionary_lookup_and_classify() -> None:
    block = {
        "values": [
            {"id": "IC50", "status": "active", "aliases": ["ic50"]},
            {"id": "DEPRECATED", "status": "deprecated"},
        ]
    }
    lookup = vocab_audit._dictionary_lookup(block)
    assert lookup["ic50"] == "alias"
    assert vocab_audit._classify(lookup.get("IC50")) == "ok"
    assert vocab_audit._classify(lookup.get("DEPRECATED")) == "deprecated"
    assert vocab_audit._classify(None) == "new"


def test_audit_vocabularies(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = {
        "meta": {"chembl_release": "v1"},
        "activity_standard_type": {"values": [{"id": "IC50", "status": "active"}]},
        "assay_type": {"values": [{"id": "binding", "status": "active"}]},
    }

    monkeypatch.setattr(vocab_audit, "load_vocab_store", lambda _: store)
    monkeypatch.setattr(vocab_audit, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(vocab_audit, "new_client", DummyClient())
    monkeypatch.setattr(
        vocab_audit,
        "FIELD_SPECS",
        (
            vocab_audit.FieldSpec(
                dictionary="activity_standard_type",
                resource="activity",
                field="standard_type",
                only="standard_type",
            ),
            vocab_audit.FieldSpec(
                dictionary="assay_type",
                resource="assay",
                field="assay_type",
                only="assay_type",
            ),
        ),
    )
    monkeypatch.setattr(vocab_audit, "_git_commit", lambda: "test")

    output = tmp_path / "out.csv"
    meta = tmp_path / "meta.yaml"
    result = vocab_audit.audit_vocabularies(store=tmp_path, output=output, meta=meta, pages=1, page_size=5)
    assert result.output.exists()
    assert result.meta.exists()
    csv_content = result.output.read_text(encoding="utf-8")
    assert "activity_standard_type" in csv_content
    assert "assay_type" in csv_content
    meta_content = result.meta.read_text(encoding="utf-8")
    assert "pipeline_version" in meta_content

