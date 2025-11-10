"""Дополнительные тесты для клиентов chembl_entities."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from typing import Any, cast

import pandas as pd
import pytest

from bioetl.clients.chembl_entities import (
    ChemblAssayClassificationEntityClient,
    ChemblAssayClassMapEntityClient,
    ChemblAssayParametersEntityClient,
    ChemblCompoundRecordEntityClient,
    ChemblDataValidityEntityClient,
    ChemblMoleculeEntityClient,
)


class _RecordingChemblClient:
    """Простая заглушка paginate для проверки параметров вызова."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any] | None, int, str | None]] = []
        self.payloads: list[dict[str, Any]] = []
        self.should_raise: bool = False

    def paginate(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_size: int = 200,
        items_key: str | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        snapshot = dict(params) if params is not None else None
        self.calls.append((endpoint, snapshot, page_size, items_key))
        if self.should_raise:
            raise RuntimeError("boom")
        for item in self.payloads:
            yield dict(item)


class _DummyLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: str, **payload: Any) -> None:
        self.events.append((event, payload))

    def warning(self, event: str, **payload: Any) -> None:
        self.events.append((event, payload))

    def debug(self, event: str, **payload: Any) -> None:
        self.events.append((event, payload))

    def bind(self, **_: Any) -> _DummyLogger:
        return self


class _DummyUnifiedLogger:
    def __init__(self) -> None:
        self.logger = _DummyLogger()

    def configure(self) -> None:
        return None

    def get(self, _: str) -> _DummyLogger:
        return self.logger


@pytest.fixture(autouse=True)
def patch_unified_logger(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "bioetl.clients.chembl_entities.UnifiedLogger",
        _DummyUnifiedLogger(),
    )


def test_entity_clients_issue_expected_requests() -> None:
    molecule_stub = _RecordingChemblClient()
    molecule_client = ChemblMoleculeEntityClient(molecule_stub)
    molecule_client.fetch_by_ids(ids=["CHEMBL1"], fields=[], page_limit=25)
    molecule_call = molecule_stub.calls[0]
    assert molecule_call[0] == "/molecule.json"
    assert molecule_call[3] == "molecules"

    validity_stub = _RecordingChemblClient()
    validity_client = ChemblDataValidityEntityClient(validity_stub)
    validity_client.fetch_by_ids(ids=["comment"], fields=[], page_limit=10)
    validity_call = validity_stub.calls[0]
    assert validity_call[0] == "/data_validity_lookup.json"
    assert validity_call[3] == "data_validity_lookups"

    class_map_stub = _RecordingChemblClient()
    class_map_client = ChemblAssayClassMapEntityClient(class_map_stub)
    class_map_client.fetch_by_ids(ids=["A1"], fields=[], page_limit=10)
    class_map_call = class_map_stub.calls[0]
    assert class_map_call[0] == "/assay_class_map.json"
    assert class_map_call[3] == "assay_class_maps"

    classification_stub = _RecordingChemblClient()
    classification_client = ChemblAssayClassificationEntityClient(classification_stub)
    classification_client.fetch_by_ids(ids=["C1"], fields=[], page_limit=5)
    classification_call = classification_stub.calls[0]
    assert classification_call[0] == "/assay_classification.json"
    assert classification_call[2] == 5


def test_assay_parameters_client_returns_empty_for_invalid_ids() -> None:
    chembl_client = _RecordingChemblClient()
    client = ChemblAssayParametersEntityClient(chembl_client)

    invalid_ids: list[Any] = [None, float("nan"), ""]
    result = client.fetch_by_ids(
        ids=cast(Iterable[str], invalid_ids),
        fields=[],
        active_only=True,
    )

    assert result == {}
    assert chembl_client.calls == []


def test_assay_parameters_client_respects_active_and_fields() -> None:
    chembl_client = _RecordingChemblClient()
    chembl_client.payloads = [
        {"assay_chembl_id": "A1", "name": "param", "active": 1},
        {"assay_chembl_id": "A1", "name": "param-2", "active": 0},
    ]
    client = ChemblAssayParametersEntityClient(chembl_client)

    result = client.fetch_by_ids(
        ids=["A1", "A1"],
        fields=["assay_chembl_id", "name"],
        page_limit=50,
        active_only=False,
    )

    assert set(result.keys()) == {"A1"}
    assert chembl_client.calls == [
        (
            "/assay_parameter.json",
            {
                "assay_chembl_id__in": "A1",
                "limit": 50,
                "only": "assay_chembl_id,name",
            },
            50,
            "assay_parameters",
        )
    ]


def test_assay_parameters_client_filters_nan_ids() -> None:
    chembl_client = _RecordingChemblClient()
    chembl_client.payloads = [{"assay_chembl_id": "B1"}]
    client = ChemblAssayParametersEntityClient(chembl_client)

    ids_raw: list[Any] = ["B1", pd.Series([None])[0], "  B1  "]
    result = client.fetch_by_ids(
        ids=cast(Iterable[str], ids_raw),
        fields=[],
        page_limit=10,
    )

    assert "B1" in result
    assert len(chembl_client.calls) == 1


def test_compound_record_client_returns_empty_when_no_pairs() -> None:
    chembl_client = _RecordingChemblClient()
    client = ChemblCompoundRecordEntityClient(chembl_client)

    result = client.fetch_by_pairs(pairs=[], fields=[], page_limit=100)

    assert result == {}
    assert chembl_client.calls == []


def test_compound_record_client_deduplicates_by_priority() -> None:
    chembl_client = _RecordingChemblClient()
    chembl_client.payloads = [
        {
            "molecule_chembl_id": "C1",
            "document_chembl_id": "D1",
            "curated": False,
            "removed": True,
            "record_id": 20,
        },
        {
            "molecule_chembl_id": "C1",
            "document_chembl_id": "D1",
            "curated": True,
            "removed": True,
            "record_id": 10,
        },
        {
            "molecule_chembl_id": "C1",
            "document_chembl_id": "D1",
            "curated": True,
            "removed": False,
            "record_id": 30,
        },
    ]

    client = ChemblCompoundRecordEntityClient(chembl_client)

    result = client.fetch_by_pairs(
        pairs=[("C1", "D1"), ("C1", "D1")],
        fields=["molecule_chembl_id", "document_chembl_id", "curated", "removed", "record_id"],
    )

    assert result == {
        ("C1", "D1"): {
            "molecule_chembl_id": "C1",
            "document_chembl_id": "D1",
            "curated": True,
            "removed": False,
            "record_id": 30,
        }
    }
    assert chembl_client.calls


def test_compound_record_client_handles_paginate_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    chembl_client = _RecordingChemblClient()
    chembl_client.should_raise = True

    client = ChemblCompoundRecordEntityClient(chembl_client)

    result = client.fetch_by_pairs(
        pairs=[("M1", "D1"), ("M2", "D1")],
        fields=[],
        page_limit=10,
    )

    assert result == {}

