"""Unit tests covering the OpenAlex merge helper."""

from __future__ import annotations

import pandas as pd

from bioetl.sources.openalex.merge import merge_openalex_with_base


def test_merge_openalex_with_base_prefers_doi_join() -> None:
    base = pd.DataFrame(
        {
            "chembl_doi": ["10.1000/one", "10.1000/two"],
            "chembl_pmid": [1234, 5678],
        }
    )
    openalex = pd.DataFrame(
        {
            "doi_clean": ["10.1000/one"],
            "title": ["Example"],
            "journal": ["Journal"],
            "openalex_id": ["W1"],
            "openalex_pmid": [1234],
            "openalex_is_oa": [True],
            "open_access": [{"is_oa": True}],
            "publication_date": ["2024-01-15"],
        }
    )

    merged = merge_openalex_with_base(base, openalex)

    assert "openalex_title" in merged.columns
    assert merged.loc[0, "openalex_title"] == "Example"
    assert pd.isna(merged.loc[1, "openalex_title"])
    assert bool(merged.loc[0, "openalex_is_oa"]) is True
    assert merged.loc[0, "openalex_year"] == 2024


def test_merge_openalex_with_base_falls_back_to_pmid() -> None:
    base = pd.DataFrame({"chembl_doi": [None], "chembl_pmid": [9999]})
    openalex = pd.DataFrame({"openalex_pmid": [9999], "title": ["PMID Match"]})

    merged = merge_openalex_with_base(base, openalex)

    assert merged.loc[0, "openalex_title"] == "PMID Match"


def test_merge_openalex_detects_conflicts() -> None:
    base = pd.DataFrame({"chembl_doi": ["10.1/abc"], "chembl_pmid": [None]})
    openalex = pd.DataFrame(
        {
            "doi_clean": ["10.1/wrong"],
            "openalex_doi": ["10.1/wrong"],
            "title": ["Conflict"],
        }
    )

    merged = merge_openalex_with_base(base, openalex, conflict_detection=True)

    assert "conflict_openalex_doi" in merged.columns
    assert bool(merged.loc[0, "conflict_openalex_doi"]) is True
