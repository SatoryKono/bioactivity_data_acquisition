"""Tests for Crossref merge helpers."""

from __future__ import annotations

import pandas as pd

from bioetl.sources.crossref.merge import merge_crossref_with_base


def test_merge_crossref_with_base_joins_on_normalised_doi() -> None:
    """Crossref data should merge even when DOI case differs."""

    base_df = pd.DataFrame(
        {
            "chembl_doi": ["10.1000/ABC123"],
            "chembl_title": ["ChEMBL"],
        }
    )
    crossref_df = pd.DataFrame(
        {
            "doi_clean": ["10.1000/abc123"],
            "title": ["Crossref Title"],
            "journal": ["Journal"],
        }
    )

    merged = merge_crossref_with_base(base_df, crossref_df)

    assert merged.loc[0, "crossref_title"] == "Crossref Title"
    assert merged.loc[0, "crossref_journal"] == "Journal"
    assert merged.loc[0, "crossref_doi"] == "10.1000/abc123"
    assert bool(merged.loc[0, "conflict_crossref_doi"]) is False


def test_merge_crossref_with_base_detects_conflicts() -> None:
    """A differing Crossref DOI should trigger the conflict flag."""

    base_df = pd.DataFrame({"chembl_doi": ["10.1000/base"]})
    crossref_df = pd.DataFrame(
        {
            "doi_clean": ["10.1000/base"],
            "crossref_doi": ["10.1000/alternate"],
        }
    )

    merged = merge_crossref_with_base(base_df, crossref_df)

    assert merged.loc[0, "crossref_doi"] == "10.1000/alternate"
    assert bool(merged.loc[0, "conflict_crossref_doi"]) is True
