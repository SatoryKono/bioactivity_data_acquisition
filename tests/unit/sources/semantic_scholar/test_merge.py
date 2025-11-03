"""Tests for the Semantic Scholar merge policy helpers."""

from __future__ import annotations

import pandas as pd

from bioetl.sources.semantic_scholar.merge import merge_semantic_scholar_with_base


def test_merge_by_doi() -> None:
    """Merging uses DOI as the primary join key when available."""

    base_df = pd.DataFrame(
        {
            "chembl_doi": ["10.1000/xyz"],
            "chembl_pmid": [pd.NA],
        }
    )

    semantic_df = pd.DataFrame(
        {
            "doi_clean": ["10.1000/xyz"],
            "paper_id": ["paper-1"],
            "pubmed_id": ["12345"],
            "title": ["Sample title"],
            "authors": ["Author One"],
            "year": [2023],
            "citation_count": [42],
            "is_oa": [True],
            "publication_types": [["journalarticle"]],
        }
    )

    merged = merge_semantic_scholar_with_base(base_df, semantic_df)

    assert merged["semantic_scholar_paper_id"].iloc[0] == "paper-1"
    assert merged["semantic_scholar_title"].iloc[0] == "Sample title"
    assert merged["semantic_scholar_pmid"].iloc[0] == 12345
    assert "semantic_scholar_pubmed_id" not in merged.columns
    assert bool(merged["conflict_semantic_scholar_doi"].iloc[0]) is False


def test_merge_falls_back_to_pmid() -> None:
    """When DOI is missing the helper joins on the PMID column."""

    base_df = pd.DataFrame(
        {
            "chembl_doi": [pd.NA],
            "chembl_pmid": [123456],
        }
    )

    semantic_df = pd.DataFrame(
        {
            "doi_clean": [pd.NA],
            "paper_id": ["paper-2"],
            "pubmed_id": ["123456"],
            "title": ["PMID match"],
        }
    )

    merged = merge_semantic_scholar_with_base(base_df, semantic_df)

    assert merged["semantic_scholar_paper_id"].iloc[0] == "paper-2"
    assert merged["semantic_scholar_title"].iloc[0] == "PMID match"
    assert merged["semantic_scholar_pmid"].iloc[0] == 123456


def test_merge_uses_title_fallback() -> None:
    """Title-based matching is used when neither DOI nor PMID are available."""

    base_df = pd.DataFrame(
        {
            "chembl_doi": [pd.NA],
            "chembl_pmid": [pd.NA],
            "chembl_title": ["Deterministic Title"],
        }
    )

    semantic_df = pd.DataFrame(
        {
            "paper_id": ["paper-3"],
            "title": ["Deterministic Title"],
            "_title_for_join": ["deterministic title"],
        }
    )

    merged = merge_semantic_scholar_with_base(
        base_df,
        semantic_df,
        base_title_column="chembl_title",
    )

    assert merged["semantic_scholar_paper_id"].iloc[0] == "paper-3"
    assert merged["semantic_scholar_title"].iloc[0] == "Deterministic Title"
    assert "semantic_scholar_title_for_join" not in merged.columns
