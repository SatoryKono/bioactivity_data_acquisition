from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Iterator, Mapping, cast

import pytest

from bioetl.clients.client_chembl_iterator import ChemblEntityIteratorBase
from bioetl.clients.entities.client_compound_record import (
    ChemblCompoundRecordEntityClient,
    _compound_record_dedup_priority,
    _safe_bool,
)
from bioetl.clients.entities.client_document import ChemblDocumentClient
from bioetl.clients.entities.client_testitem import ChemblTestitemClient


class RecordingChemblClient:
    """Stub Chembl client that records paginate calls and yields configured payloads."""

    def __init__(
        self,
        responses: dict[tuple[str, tuple[str, ...]], list[dict[str, Any]]],
        *,
        fail_on: set[str] | None = None,
    ) -> None:
        self._responses = responses
        self._fail_on = fail_on or set()
        self.calls: list[tuple[str, Mapping[str, Any]]] = []

    def paginate(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_size: int = 200,
        items_key: str | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        params = params or {}
        self.calls.append((endpoint, params))
        doc_id = params.get("document_chembl_id")
        if not isinstance(doc_id, str):
            return iter(())
        if doc_id in self._fail_on:
            raise RuntimeError(f"failed for {doc_id}")
        chunk_raw = params.get("molecule_chembl_id__in", "")
        chunk = tuple(str(part) for part in str(chunk_raw).split(",") if part)
        records = list(self._responses.get((doc_id, chunk), []))
        return iter(records)


class DummyChemblClient:
    """Minimal Chembl client stub satisfying ChemblClientProtocol."""

    def paginate(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_size: int = 200,
        items_key: str | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        return iter(())


def test_fetch_by_pairs_filters_invalid_pairs(monkeypatch: pytest.MonkeyPatch) -> None:
    client = ChemblCompoundRecordEntityClient(chembl_client=DummyChemblClient())

    # Empty result with log path exercised.
    result = client.fetch_by_pairs(
        pairs=cast(
            list[tuple[str, str]],
            [("M1", None), ("", "D1"), ("M2", float("nan"))],
        ),
        fields=["record_id"],
        chunk_size=1,
    )
    assert result.empty
    assert result.columns.tolist() == ["record_id"]


def test_fetch_by_pairs_chunking_and_dedup(monkeypatch: pytest.MonkeyPatch) -> None:
    responses: dict[tuple[str, tuple[str, ...]], list[dict[str, Any]]] = {
        ("D1", ("M1",)): [
            {"molecule_chembl_id": "M1", "document_chembl_id": "D1", "curated": False, "removed": False, "record_id": "3"},
            {"molecule_chembl_id": "M1", "document_chembl_id": "D1", "curated": True, "removed": False, "record_id": "2"},
        ],
        ("D1", ("M2",)): [
            {"molecule_chembl_id": "M2", "document_chembl_id": "D1", "curated": False, "removed": True, "record_id": "7"},
            {"molecule_chembl_id": "M2", "document_chembl_id": "D1", "curated": False, "removed": False, "record_id": "5"},
        ],
        ("D1", ("M3",)): [
            {"molecule_chembl_id": "M3", "document_chembl_id": "D1", "curated": False, "removed": False, "record_id": "9"},
            {"molecule_chembl_id": "M3", "document_chembl_id": "D1", "curated": False, "removed": False, "record_id": "4"},
        ],
    }
    chembl_client = RecordingChemblClient(responses, fail_on={"D2"})

    client = ChemblCompoundRecordEntityClient(chembl_client=chembl_client)
    result = client.fetch_by_pairs(
        pairs=[("M1", "D1"), ("M2", "D1"), ("M3", "D1"), ("M4", "D2")],
        fields=["record_id", "curated", "removed"],
        page_limit=10,
        chunk_size=1,
    )

    assert {"record_id", "curated", "removed", "molecule_chembl_id", "document_chembl_id"}.issubset(
        set(result.columns)
    )

    def _lookup_record(molecule: str, document: str) -> dict[str, Any]:
        mask = (result["molecule_chembl_id"] == molecule) & (result["document_chembl_id"] == document)
        matched = result.loc[mask]
        assert len(matched) == 1
        return cast(dict[str, Any], matched.iloc[0].to_dict())

    # Curated record wins.
    assert _lookup_record("M1", "D1")["record_id"] == "2"
    # Removed False wins.
    assert _lookup_record("M2", "D1")["record_id"] == "5"
    # Lowest record_id wins when other priorities tie.
    assert _lookup_record("M3", "D1")["record_id"] == "4"
    # Failing document is ignored gracefully.
    assert not (
        (result["molecule_chembl_id"] == "M4") & (result["document_chembl_id"] == "D2")
    ).any()

    # Multiple chunked calls recorded.
    assert chembl_client.calls
    called_docs = {params["document_chembl_id"] for _, params in chembl_client.calls}
    assert called_docs == {"D1", "D2"}


def test_fetch_by_pairs_invalid_chunk_size() -> None:
    client = ChemblCompoundRecordEntityClient(chembl_client=DummyChemblClient())
    with pytest.raises(ValueError):
        client.fetch_by_pairs(pairs=[], fields=[], chunk_size=0)


@pytest.mark.parametrize(
    ("existing", "new", "expected_record_id"),
    [
        ({"curated": False, "removed": False, "record_id": "10"}, {"curated": True, "removed": False, "record_id": "9"}, "9"),
        ({"curated": True, "removed": False, "record_id": "8"}, {"curated": False, "removed": False, "record_id": "6"}, "8"),
        ({"curated": False, "removed": False, "record_id": "10"}, {"curated": False, "removed": True, "record_id": "5"}, "10"),
        ({"curated": False, "removed": False, "record_id": "12"}, {"curated": False, "removed": False, "record_id": "4"}, "4"),
    ],
)
def test_compound_record_dedup_priority(
    existing: dict[str, Any],
    new: dict[str, Any],
    expected_record_id: str,
) -> None:
    chosen = _compound_record_dedup_priority(existing, new)
    assert chosen["record_id"] == expected_record_id


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, False),
        (True, True),
        (0, False),
        (1, True),
        ("on", True),
        ("false", False),
        ("random", False),
        ([1], True),
    ],
)
def test_safe_bool(value: Any, expected: bool) -> None:
    assert _safe_bool(value) is expected


def test_document_client_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_init(self, chembl_client: Any, *, config: Any, batch_size: int, max_url_length: int | None) -> None:  # type: ignore[override]
        captured["config"] = config
        captured["batch_size"] = batch_size
        captured["max_url_length"] = max_url_length

    monkeypatch.setattr(
        ChemblEntityIteratorBase,
        "__init__",
        fake_init,
    )

    ChemblDocumentClient(chembl_client=SimpleNamespace(), batch_size=7, max_url_length=42)

    config = captured["config"]
    assert config.endpoint == "/document.json"
    assert captured["batch_size"] == 7
    assert captured["max_url_length"] == 42


def test_testitem_client_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_init(self, chembl_client: Any, *, config: Any, batch_size: int, max_url_length: int | None) -> None:  # type: ignore[override]
        captured["config"] = config
        captured["batch_size"] = batch_size
        captured["max_url_length"] = max_url_length

    monkeypatch.setattr(
        ChemblEntityIteratorBase,
        "__init__",
        fake_init,
    )

    ChemblTestitemClient(chembl_client=SimpleNamespace(), batch_size=11, max_url_length=None)

    config = captured["config"]
    assert config.endpoint == "/molecule.json"
    assert config.filter_param == "molecule_chembl_id__in"
    assert captured["batch_size"] == 11
    assert captured["max_url_length"] is None

