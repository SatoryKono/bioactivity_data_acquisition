"""Tests for PubMed merge policies."""

from __future__ import annotations

import pandas as pd

from bioetl.sources.pubmed.merge import merge_pubmed_with_base


def test_merge_pubmed_with_base_joins_on_pmid() -> None:
    base_df = pd.DataFrame(
        {
            "document_chembl_id": ["CHEMBL1"],
            "chembl_pmid": pd.Series([123456], dtype="Int64"),
            "chembl_doi": ["10.1000/chembl"],
        }
    )

    pubmed_df = pd.DataFrame(
        {
            "pubmed_pmid": pd.Series([123456], dtype="Int64"),
            "title": ["PubMed Title"],
            "abstract": ["PubMed Abstract"],
            "journal": ["Journal"],
            "journal_abbrev": ["J."],
            "authors": ["Author, A"],
            "doi_clean": ["10.1000/pubmed"],
            "pubmed_doi": ["10.1000/pubmed"],
            "volume": ["12"],
            "issue": ["2"],
            "first_page": ["100"],
            "last_page": ["108"],
            "year": pd.Series([2020], dtype="Int64"),
            "mesh_descriptors": ["A | B"],
            "mesh_qualifiers": ["C"],
            "chemical_list": ["Chem1"],
        }
    )

    merged = merge_pubmed_with_base(base_df, pubmed_df)

    assert merged.loc[0, "pubmed_article_title"] == "PubMed Title"
    assert merged.loc[0, "pubmed_abstract"] == "PubMed Abstract"
    assert merged.loc[0, "pubmed_journal"] == "Journal"
    assert merged.loc[0, "pubmed_pmid"] == 123456
    assert merged.loc[0, "pmid"] == 123456
    assert merged.loc[0, "pubmed_mesh_descriptors"] == "A | B"
    assert merged.loc[0, "pubmed_chemical_list"] == "Chem1"


def test_merge_pubmed_with_base_fallbacks_to_doi() -> None:
    base_df = pd.DataFrame(
        {
            "document_chembl_id": ["CHEMBL2"],
            "chembl_doi": ["10.1000/pubmed"],
        }
    )

    pubmed_df = pd.DataFrame(
        {
            "pmid": pd.Series([9999], dtype="Int64"),
            "doi_clean": ["10.1000/pubmed"],
            "title": ["Fallback Title"],
        }
    )

    merged = merge_pubmed_with_base(base_df, pubmed_df)

    assert merged.loc[0, "pubmed_article_title"] == "Fallback Title"
    assert merged.loc[0, "doi_clean"] == "10.1000/pubmed"


def test_merge_pubmed_with_base_handles_missing_keys() -> None:
    base_df = pd.DataFrame({"document_chembl_id": ["CHEMBL3"]})
    pubmed_df = pd.DataFrame({"title": ["Missing Keys"]})

    merged = merge_pubmed_with_base(base_df, pubmed_df)

    assert "pubmed_article_title" not in merged.columns
    assert merged.equals(base_df)
