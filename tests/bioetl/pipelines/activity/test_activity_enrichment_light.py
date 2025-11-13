"""Lightweight smoke tests for activity enrichment with stubbed data."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from bioetl.pipelines.activity import activity_enrichment


class _StubAssayClient:
    def __init__(self, payload: dict[str, dict[str, Any]]) -> None:
        self.payload = payload
        self.calls: list[dict[str, Any]] = []

    def fetch_assays_by_ids(
        self,
        *,
        ids: list[str],
        fields: list[str],
        page_limit: int,
    ) -> dict[str, dict[str, Any]]:
        self.calls.append({"ids": ids, "fields": fields, "page_limit": page_limit})
        return self.payload


@pytest.mark.unit
def test_enrich_with_assay_empty_dataframe(patch_unified_logger) -> None:
    """Empty frame short-circuits and still exposes the deterministic columns."""

    patch_unified_logger(activity_enrichment)
    df = pd.DataFrame({"assay_chembl_id": pd.Series(dtype="string")})
    client = _StubAssayClient(payload={})

    result = activity_enrichment.enrich_with_assay(df, client, cfg={})

    assert list(result.columns) == ["assay_chembl_id", "assay_organism", "assay_tax_id"]
    assert client.calls == []


@pytest.mark.unit
def test_enrich_with_assay_missing_key_column(patch_unified_logger) -> None:
    """Missing key column prevents network calls and preserves the input frame."""

    patch_unified_logger(activity_enrichment)
    df = pd.DataFrame({"unrelated": [1, 2, 3]})
    client = _StubAssayClient(payload={"CHEMBL1": {"assay_organism": "Human", "assay_tax_id": 9606}})

    result = activity_enrichment.enrich_with_assay(df, client, cfg={})

    assert "assay_organism" in result.columns
    assert "assay_tax_id" in result.columns
    assert client.calls == []


@pytest.mark.unit
def test_enrich_with_assay_successful_merge(tmp_path, patch_unified_logger) -> None:
    """Stubbed client supplies enrichment data merged back into the original order."""

    patch_unified_logger(activity_enrichment)
    df = pd.DataFrame(
        {
            "assay_chembl_id": pd.Series(["CHEMBL1", "CHEMBL2"], dtype="string"),
            "assay_organism": pd.Series([pd.NA, pd.NA], dtype="string"),
        }
    )
    client = _StubAssayClient(
        payload={
            "CHEMBL1": {"assay_organism": "Human", "assay_tax_id": 9606},
            "CHEMBL2": {"assay_organism": "Mouse", "assay_tax_id": "10090"},
        }
    )

    cfg = {"fields": ["assay_chembl_id"], "page_limit": 25}
    result = activity_enrichment.enrich_with_assay(df, client, cfg)

    assert client.calls[0]["ids"] == ["CHEMBL1", "CHEMBL2"]
    assert set(client.calls[0]["fields"]) >= {"assay_chembl_id", "assay_organism", "assay_tax_id"}
    assert result.loc[0, "assay_organism"] == "Human"
    assert result.loc[1, "assay_tax_id"] == 10090
    assert result["assay_tax_id"].dtype == "Int64"

