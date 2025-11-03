"""Conflict detection tests for document merge outputs."""

from __future__ import annotations

import pandas as pd

from bioetl.sources.document.merge.policy import detect_conflicts


def test_detect_conflicts_flags_divergent_doi_values() -> None:
    """Different DOI values across sources should be marked as a conflict."""

    row = pd.Series(
        {
            "chembl_doi": "10.1/primary",
            "pubmed_doi": "10.1/alternate",
            "crossref_doi": "10.1/alternate",
        }
    )

    result = detect_conflicts(row.copy())

    assert result["conflict_doi"] is True


def test_detect_conflicts_ignores_blank_and_zero_pmids() -> None:
    """Empty or zero PMID entries should be ignored when checking conflicts."""

    row = pd.Series(
        {
            "chembl_pmid": "000000",
            "pubmed_pmid": None,
            "openalex_pmid": "",
            "semantic_scholar_pmid": "123456",
        }
    )

    result = detect_conflicts(row.copy())

    assert result["conflict_pmid"] is False
