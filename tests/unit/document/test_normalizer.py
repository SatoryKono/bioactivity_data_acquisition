"""Tests for the document normalisation helpers."""

from __future__ import annotations

import pandas as pd

from bioetl.sources.chembl.document.normalizer import normalize_document_frame


def test_normalize_document_frame_applies_expected_mappings() -> None:
    """Normalisation should produce ChEMBL-prefixed columns and metadata."""

    frame = pd.DataFrame(
        {
            "document_chembl_id": [" chembl12 "],
            "pubmed_id": ["12345"],
            "classification": ["primary"],
            "is_experimental_doc": ["yes"],
            "document_contains_external_links": ["no"],
            "title": ["Title with Spaces"],
            "journal": ["Journal"],
            "year": ["2020"],
            "authors": ["A;B"],
            "abstract": ["Lorem"],
            "volume": ["1"],
            "issue": ["2"],
            "first_page": [" 101 "],
            "last_page": [" 110 "],
            "doi": ["10.1000/xyz"],
        }
    )

    normalised = normalize_document_frame(frame)

    assert normalised.loc[0, "document_chembl_id"] == "CHEMBL12"
    assert normalised.loc[0, "document_pubmed_id"] == 12345
    assert normalised.loc[0, "chembl_pmid"] == 12345
    assert normalised.loc[0, "chembl_doc_type"] == "journal-article"
    assert normalised.loc[0, "chembl_title"] == "Title with Spaces"
    assert normalised.loc[0, "chembl_year"] == "2020"
    assert normalised.loc[0, "chembl_journal"] == "Journal"
    assert normalised.loc[0, "chembl_doi"] == "10.1000/xyz"
    assert normalised.loc[0, "chembl_first_page"] == "101"
    assert normalised.loc[0, "chembl_last_page"] == "110"
    assert "title" not in normalised.columns
    assert normalised.loc[0, "_original_title"] == "Title with Spaces"


def test_normalize_document_frame_handles_empty_frame() -> None:
    """An empty dataframe should be returned unchanged."""

    empty = pd.DataFrame(columns=["document_chembl_id"])
    result = normalize_document_frame(empty)
    assert result.equals(empty)
