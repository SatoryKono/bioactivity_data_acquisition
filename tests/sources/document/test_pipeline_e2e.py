"""Lightweight end-to-end test for document merge policy."""

from __future__ import annotations

import pandas as pd

from bioetl.sources.document.merge.policy import merge_with_precedence


def test_merge_policy_combines_multiple_sources() -> None:
    """Document merge should consolidate bibliographic information."""

    chembl_df = pd.DataFrame(
        {
            "document_chembl_id": ["CHEMBL123"],
            "chembl_doi": ["10.1000/chembl"],
            "chembl_pmid": [9876543],
            "chembl_title": ["Original"],
        }
    )
    pubmed_df = pd.DataFrame(
        {
            "pubmed_id": [9876543],
            "doi": ["10.1000/pubmed"],
            "article_title": ["PubMed Title"],
            "pubmed_journal": ["Journal"],
        }
    )
    crossref_df = pd.DataFrame(
        {
            "doi_clean": ["10.1000/crossref"],
            "title": ["Crossref Title"],
            "journal": ["CR Journal"],
        }
    )

    merged = merge_with_precedence(
        chembl_df,
        pubmed_df=pubmed_df,
        crossref_df=crossref_df,
    )

    assert merged.loc[0, "doi_clean"] == "10.1000/crossref"
    assert merged.loc[0, "title"] == "PubMed Title"
    assert merged.loc[0, "journal"] == "Journal"
    assert merged.loc[0, "pmid"] == 9876543
