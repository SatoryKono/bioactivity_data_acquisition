"""Дополнительные тесты для клиентов chembl_entities."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd
import pytest

import pytest

from bioetl.clients.chembl_entities import (
    ChemblAssayParametersEntityClient,
    ChemblAssayClassificationEntityClient,
    ChemblAssayClassMapEntityClient,
    ChemblDataValidityEntityClient,
    ChemblMoleculeEntityClient,
    ChemblCompoundRecordEntityClient,
    _safe_bool,
)


class _RecordingChemblClient:
    """Простая заглушка paginate для проверки параметров вызова."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], int, str]] = []
        self.payloads: list[dict[str, Any]] = []
        self.should_raise: bool = False

    def paginate(
        self,
        endpoint: str,
        *,
        params: dict[str, Any],
        page_size: int,
        items_key: str,
    ) -> Iterable[dict[str, Any]]:
        self.calls.append((endpoint, params, page_size, items_key))
        if self.should_raise:
            raise RuntimeError("boom")
        yield from [{**item} for item in self.payloads]


class _DummyLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def info(self, event: str, **payload: Any) -> None:
        self.events.append((event, payload))

    def warning(self, event: str, **payload: Any) -> None:
        self.events.append((event, payload))

    def debug(self, event: str, **payload: Any) -> None:
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
def patch_unified_logger(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "bioetl.clients.chembl_entities.UnifiedLogger",
        _DummyUnifiedLogger(),
    )


def test_entity_clients_initialise_configs(monkeypatch: pytest.MonkeyPatch) -> None:
    dummy = _RecordingChemblClient()
    molecule_client = ChemblMoleculeEntityClient(dummy)
    validity_client = ChemblDataValidityEntityClient(dummy)
    class_map_client = ChemblAssayClassMapEntityClient(dummy)
    classification_client = ChemblAssayClassificationEntityClient(dummy)

    assert molecule_client._config.endpoint == "/molecule.json"
    assert validity_client._config.items_key == "data_validity_lookups"
    assert class_map_client._config.supports_list_result is True
    assert classification_client._config.filter_param == "assay_class_id__in"


def test_assay_parameters_client_returns_empty_for_invalid_ids() -> None:
    chembl_client = _RecordingChemblClient()
    client = ChemblAssayParametersEntityClient(chembl_client)

    result = client.fetch_by_ids(ids=[None, float("nan"), ""], fields=[], active_only=True)

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
            "/assay_parameters.json",
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

    ids = ["B1", pd.Series([None])[0], "  B1  "]
    result = client.fetch_by_ids(ids=ids, fields=[], page_limit=10)

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


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, False),
        (True, True),
        (False, False),
        (1, True),
        (0, False),
        ("TRUE", True),
        ("no", False),
    ],
)
def test_safe_bool_coercion(value: Any, expected: bool) -> None:
    assert _safe_bool(value) is expected

