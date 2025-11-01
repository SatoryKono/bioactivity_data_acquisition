"""Tests for document merge logic."""

from __future__ import annotations

import pandas as pd

from bioetl.sources.document.merge.policy import merge_with_precedence


def test_merge_with_precedence_prefers_crossref_doi() -> None:
    """Crossref DOI should override the baseline ChEMBL DOI when available."""

    chembl_df = pd.DataFrame(
        {
            "document_chembl_id": ["CHEMBL1"],
            "chembl_doi": ["10.1000/original"],
            "chembl_pmid": [123456],
        }
    )
    crossref_df = pd.DataFrame(
        {
            "doi_clean": ["10.1000/crossref"],
            "title": ["Crossref Title"],
        }
    )

    merged = merge_with_precedence(chembl_df, crossref_df=crossref_df)

    assert merged.loc[0, "doi_clean"] == "10.1000/crossref"
    assert merged.loc[0, "doi_clean_source"] == "crossref"


def test_merge_with_precedence_handles_missing_sources() -> None:
    """Merging gracefully handles missing optional sources."""

    chembl_df = pd.DataFrame({"document_chembl_id": [], "chembl_doi": []})

    merged = merge_with_precedence(chembl_df)

    assert list(merged.columns)  # Columns exist even when no rows are present.
    assert "doi_clean" in merged.columns
    assert "conflict_doi" in merged.columns
