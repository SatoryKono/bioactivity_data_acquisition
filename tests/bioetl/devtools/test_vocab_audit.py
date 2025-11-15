from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator, Mapping

import pytest

from bioetl.devtools import cli_vocab_audit as vocab_audit
from bioetl.core.utils import VocabStoreError


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


def test_resolve_store_path_precedence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    custom = tmp_path / "custom.yaml"
    custom.write_text("{}", encoding="utf-8")
    assert vocab_audit._resolve_store_path(custom) == custom.resolve()

    aggregated = tmp_path / "aggregated.yaml"
    aggregated.write_text("{}", encoding="utf-8")
    legacy = tmp_path / "legacy.yaml"
    legacy.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(vocab_audit, "DEFAULT_AGGREGATED", aggregated)
    monkeypatch.setattr(vocab_audit, "LEGACY_AGGREGATED", legacy)
    assert vocab_audit._resolve_store_path(None) == aggregated.resolve()

    aggregated.unlink()
    assert vocab_audit._resolve_store_path(None) == legacy.resolve()


def test_load_store_wraps_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def failing_loader(path: Path) -> None:
        raise VocabStoreError("boom")

    monkeypatch.setattr(vocab_audit, "load_vocab_store", failing_loader)
    with pytest.raises(RuntimeError):
        vocab_audit._load_store(Path("data.yaml"))


def test_select_mapping_entries_and_aliases() -> None:
    entries = vocab_audit._select_mapping_entries(
        [{"id": "A"}, ("tuple",), {"id": "B"}]  # type: ignore[arg-type]
    )
    assert len(entries) == 2
    aliases = list(vocab_audit._iter_aliases(["x", None, 1, ""]))
    assert aliases == ["x"]


def test_normalise_value_and_classify() -> None:
    assert vocab_audit._normalise_value(" value ") == "value"
    assert vocab_audit._normalise_value("") is None
    assert vocab_audit._classify("alias") == "alias"
    assert vocab_audit._classify("deprecated") == "deprecated"
    assert vocab_audit._classify("UNKNOWN") == "unknown"


def test_fetch_unique_values_handles_pagination(monkeypatch: pytest.MonkeyPatch) -> None:
    class Resource:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def filter(self, **filters: Any) -> DummyQuery:
            self.calls.append(filters)
            if filters["offset"] == 0:
                return DummyQuery([{"standard_type": "IC50"}])
            return DummyQuery([])

    client = DummyClient()
    client.activity = Resource()  # type: ignore[assignment]
    monkeypatch.setattr(vocab_audit, "new_client", client)

    spec = vocab_audit.FieldSpec(
        dictionary="activity_standard_type",
        resource="activity",
        field="standard_type",
        only="standard_type",
    )
    counter = vocab_audit._fetch_unique_values(spec, page_size=5, pages=2)
    assert counter["IC50"] == 1


def test_compute_business_key_hash_deterministic() -> None:
    rows = [
        {"dictionary": "a", "value": "1", "status": "ok"},
        {"dictionary": "b", "value": "2", "status": "new"},
    ]
    first = vocab_audit._compute_business_key_hash(rows)
    second = vocab_audit._compute_business_key_hash(list(reversed(rows)))
    assert first != second


def test_is_truthy_variants() -> None:
    assert vocab_audit._is_truthy("YES")
    assert not vocab_audit._is_truthy("0")
    assert not vocab_audit._is_truthy(None)


def test_resolve_chembl_client_env_offline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vocab_audit, "UnifiedLogger", DummyUnifiedLogger)
    sentinel_client = DummyClient()
    monkeypatch.setattr(vocab_audit, "get_offline_new_client", lambda: sentinel_client)
    monkeypatch.setenv(vocab_audit.OFFLINE_CLIENT_ENV, "1")
    client = vocab_audit._resolve_chembl_client()
    assert client is sentinel_client


def test_resolve_chembl_client_prefers_online(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vocab_audit, "UnifiedLogger", DummyUnifiedLogger)
    sentinel_api_client = object()

    class DummyChemblClient:
        def __init__(self, client: Any, *args: Any, **kwargs: Any) -> None:
            self.client = client

        def paginate(
            self,
            endpoint: str,
            *,
            params: Mapping[str, Any] | None = None,
            page_size: int | None = None,
        ) -> Iterator[Mapping[str, Any]]:
            del endpoint, params, page_size
            return iter([])

    monkeypatch.setattr(vocab_audit, "for_tool", lambda **_: sentinel_api_client)
    monkeypatch.setattr(vocab_audit, "ChemblClient", DummyChemblClient)
    monkeypatch.setenv(vocab_audit.OFFLINE_CLIENT_ENV, "false")
    client = vocab_audit._resolve_chembl_client()
    assert isinstance(client, vocab_audit._ChemblClientAdapter)


def test_extract_release_falls_back_to_blocks() -> None:
    store = {
        "activity": {"meta": {"chembl_release": "v2"}},
    }
    assert vocab_audit._extract_release(store) == "v2"


def test_audit_vocabularies_handles_missing_dictionary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = {"meta": {"chembl_release": "v1"}}
    monkeypatch.setattr(vocab_audit, "UnifiedLogger", DummyUnifiedLogger)
    monkeypatch.setattr(vocab_audit, "load_vocab_store", lambda _: store)
    monkeypatch.setattr(vocab_audit, "new_client", DummyClient())
    monkeypatch.setattr(vocab_audit, "_git_commit", lambda: "unknown")
    monkeypatch.setattr(
        vocab_audit,
        "FIELD_SPECS",
        (
            vocab_audit.FieldSpec(
                dictionary="missing_dictionary",
                resource="activity",
                field="standard_type",
                only="standard_type",
            ),
        ),
    )
    result = vocab_audit.audit_vocabularies(
        store=tmp_path,
        output=tmp_path / "out.csv",
        meta=tmp_path / "meta.yaml",
        pages=1,
        page_size=5,
    )
    assert result.rows == ()


def test_git_commit_returns_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        vocab_audit.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(FileNotFoundError()),
    )
    assert vocab_audit._git_commit() == "unknown"
