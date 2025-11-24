from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bioetl.chembl.repos import chembl_repositories
from bioetl.chembl.repos.chembl_repositories import (
    ChemblActivityRepository,
    ChemblCompoundRecordRepository,
    ChemblMoleculeRepository,
)


def test_activity_repository_fetches_by_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    chembl_client = MagicMock()
    captured: dict[str, Any] = {}

    class DummyActivityClient:
        DEFAULT_BATCH_SIZE = 25

        def __init__(self, client: Any, *, batch_size: int) -> None:
            captured["client"] = client
            captured["batch_size"] = batch_size

        def iterate_by_ids(self, ids, *, select_fields):
            captured["ids"] = list(ids)
            captured["fields"] = list(select_fields)
            yield {"activity_id": 1, "record_id": 10, "molecule_chembl_id": "CHEMBL1"}

    monkeypatch.setattr(chembl_repositories, "ChemblActivityClient", DummyActivityClient)

    repo = ChemblActivityRepository(chembl_client, batch_size=30)
    df = repo.fetch_by_ids(["1", None, " 2 "], fields=["activity_id", "record_id", "molecule_chembl_id"])

    assert captured["client"] is chembl_client
    assert captured["batch_size"] == 30
    assert captured["ids"] == ["1", "2"]
    assert captured["fields"] == ["activity_id", "record_id", "molecule_chembl_id"]
    assert df.to_dict(orient="list") == {
        "activity_id": [1],
        "record_id": [10],
        "molecule_chembl_id": ["CHEMBL1"],
    }


def test_compound_record_repository_paginates(monkeypatch: pytest.MonkeyPatch) -> None:
    chembl_client = MagicMock()
    paginated_calls: list[dict[str, Any]] = []

    def fake_paginate(endpoint: str, **kwargs: Any):
        paginated_calls.append({"endpoint": endpoint, **kwargs})
        yield {"record_id": "100", "compound_key": "CK1", "compound_name": "CN1"}
        yield {"record_id": 100, "compound_key": "CK1", "compound_name": "CN1"}

    chembl_client.paginate = MagicMock(side_effect=fake_paginate)  # type: ignore[attr-defined]

    repo = ChemblCompoundRecordRepository(chembl_client, page_limit=500, batch_size=2)
    records = repo.fetch_by_record_ids(["100", "100", "101"])

    assert paginated_calls[0]["endpoint"] == "/compound_record.json"
    params = paginated_calls[0]["params"]
    assert params["record_id__in"] == "100,101"
    assert params["limit"] == 500
    assert params["only"] == "record_id,compound_key,compound_name"
    assert "compound_records" == paginated_calls[0]["items_key"]
    assert records == {
        "100": {"record_id": "100", "compound_key": "CK1", "compound_name": "CN1"}
    }


def test_molecule_repository_forwards_arguments() -> None:
    chembl_client = MagicMock()
    chembl_client.fetch_molecules_by_ids.return_value = pd.DataFrame(
        [{"molecule_chembl_id": "CHEMBL1", "pref_name": "A"}]
    )

    repo = ChemblMoleculeRepository(chembl_client, page_limit=750)
    df = repo.fetch_by_ids(["CHEMBL1"], fields=["molecule_chembl_id", "pref_name"], page_limit=900)

    chembl_client.fetch_molecules_by_ids.assert_called_once_with(
        ids=["CHEMBL1"], fields=["molecule_chembl_id", "pref_name"], page_limit=900
    )
    assert not df.empty
    assert list(df.columns) == ["molecule_chembl_id", "pref_name"]
