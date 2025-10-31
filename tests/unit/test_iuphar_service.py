from __future__ import annotations

import pandas as pd
import pytest

from bioetl.sources.iuphar.service import IupharService, IupharServiceConfig


class DummyClient:
    def __init__(self, responses: dict[str, list[dict[str, object]]]):
        self._responses = responses
        self.calls: list[str] = []

    def request_json(self, path: str, params: dict[str, object] | None = None):  # noqa: D401 - test stub
        self.calls.append(path)
        payloads = self._responses.get(path, [])
        if not payloads:
            return []
        if len(payloads) == 1:
            return payloads[0]
        return payloads.pop(0)


def test_enrich_targets_produces_classification() -> None:
    service = IupharService(
        client=None,
        config=IupharServiceConfig(
            identifier_column="target_chembl_id",
            output_identifier_column="target_chembl_id",
            candidate_columns=("pref_name",),
            gene_symbol_columns=(),
            fallback_source="chembl",
        ),
    )

    targets = [
        {"targetId": 1, "name": "Test Target", "familyIds": [11]},
    ]
    families = [
        {"familyId": 1, "name": "GPCRs", "parentFamilyIds": []},
        {"familyId": 10, "name": "Class A GPCRs", "parentFamilyIds": [1]},
        {"familyId": 11, "name": "Adenosine receptors", "parentFamilyIds": [10]},
    ]

    df = pd.DataFrame({"target_chembl_id": ["CHEMBL1"], "pref_name": ["Test Target"]})

    enriched, classification_df, gold_df, metrics = service.enrich_targets(
        df,
        targets=targets,
        families=families,
    )

    assert enriched.loc[0, "iuphar_type"] == "GPCRs"
    assert enriched.loc[0, "iuphar_class"] == "Class A GPCRs"
    assert enriched.loc[0, "iuphar_subclass"] == "Adenosine receptors"
    assert metrics["enrichment_success.iuphar"] == pytest.approx(1.0)
    assert not classification_df.empty
    assert not gold_df.empty


def test_enrich_targets_records_missing_mappings() -> None:
    captured: list[dict[str, object]] = []

    def _record_missing_mapping(**payload: object) -> None:
        captured.append(payload)

    service = IupharService(
        client=None,
        config=IupharServiceConfig(
            identifier_column="target_chembl_id",
            output_identifier_column="target_chembl_id",
            candidate_columns=("pref_name",),
            gene_symbol_columns=(),
            fallback_source="chembl",
        ),
        record_missing_mapping=_record_missing_mapping,
    )

    df = pd.DataFrame({"target_chembl_id": ["CHEMBL2"], "pref_name": ["Unknown"]})

    targets = [{"targetId": 99, "name": "Different", "familyIds": []}]
    families: list[dict[str, object]] = []

    enriched, classification_df, gold_df, metrics = service.enrich_targets(
        df,
        targets=targets,
        families=families,
    )

    assert metrics["enrichment_success.iuphar"] == pytest.approx(0.0)
    assert classification_df.empty
    assert gold_df.empty
    assert captured
    assert captured[0]["status"] in {"no_candidate_names", "fallback", "no_match"}


def test_fetch_collection_handles_pagination() -> None:
    client = DummyClient(
        {
            "/targets": [
                {"results": [{"targetId": 1}, {"targetId": 1}]},
                {"results": [{"targetId": 2}]},
            ]
        }
    )

    service = IupharService(client, config=IupharServiceConfig(page_size=1))

    items = service.fetch_collection("/targets", unique_key="targetId")

    assert [item["targetId"] for item in items] == [1, 2]
    assert client.calls.count("/targets") >= 2
