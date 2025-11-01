"""OpenAlex schema coverage tests."""

from __future__ import annotations

import unittest

import pandas as pd
import pytest

from bioetl.schemas.document import DocumentSchema
from bioetl.sources.openalex.schema import OpenAlexNormalizedSchema, OpenAlexRawSchema


class TestOpenAlexSchema(unittest.TestCase):
    """Ensure OpenAlex specific columns are present in :class:`DocumentSchema`."""

    def test_document_schema_includes_openalex_columns(self) -> None:
        columns = set(DocumentSchema.get_column_order())
        expected = {
            "openalex_doi",
            "openalex_pmid",
            "openalex_issn",
            "openalex_crossref_doc_type",
        }

        self.assertTrue(expected.issubset(columns))


def test_openalex_raw_schema_accepts_minimal_payload() -> None:
    frame = pd.DataFrame(
        {
            "id": ["https://openalex.org/W1"],
            "title": ["Example"],
            "doi": ["10.1000/test"],
            "authorships": [[{"author": {"display_name": "Doe"}}]],
            "primary_location": [{"source": {"display_name": "Journal"}}],
            "publication_date": ["2024-01-01"],
            "publication_year": [2024],
            "type": ["article"],
            "language": ["en"],
            "open_access": [{"is_oa": True}],
            "concepts": [[{"display_name": "Biology"}]],
            "ids": [{"pmid": "12345"}],
            "abstract": ["Abstract"],
        }
    )

    validated = OpenAlexRawSchema.validate(frame)
    assert len(validated) == 1


def test_openalex_raw_schema_rejects_missing_identifier() -> None:
    frame = pd.DataFrame({"title": ["Missing ID"]})

    with pytest.raises(Exception):
        OpenAlexRawSchema.validate(frame)


def test_openalex_normalized_schema_validates_enriched_row() -> None:
    base_columns = {
        "index": [0],
        "hash_row": ["0" * 64],
        "hash_business_key": ["1" * 64],
        "pipeline_version": ["1.0.0"],
        "run_id": ["run-1"],
        "source_system": ["openalex"],
        "chembl_release": [None],
        "extracted_at": ["2024-01-01T00:00:00+00:00"],
    }
    record = {
        "doi_clean": ["10.1000/test"],
        "openalex_doi": ["10.1000/test"],
        "openalex_doi_clean": ["10.1000/test"],
        "openalex_id": ["W1"],
        "openalex_pmid": [1234],
        "openalex_title": ["Example"],
        "openalex_journal": ["Journal"],
        "openalex_authors": ["Doe, Jane"],
        "openalex_year": [2024],
        "openalex_month": [1],
        "openalex_day": [15],
        "openalex_publication_date": ["2024-01-15"],
        "openalex_type": ["article"],
        "openalex_language": ["en"],
        "openalex_is_oa": [True],
        "openalex_oa_status": ["gold"],
        "openalex_oa_url": ["https://example.org"],
        "openalex_issn": ["1234-5678"],
        "openalex_concepts_top3": [["Biology", "Chemistry"]],
        "openalex_landing_page": ["https://openalex.org/W1"],
        "openalex_doc_type": ["article"],
        "openalex_crossref_doc_type": ["journal-article"],
        "openalex_citation_count": [12],
    }
    frame = pd.DataFrame({**base_columns, **record})

    validated = OpenAlexNormalizedSchema.validate(frame)
    assert len(validated) == 1
    assert list(validated.loc[0, "openalex_concepts_top3"]) == ["Biology", "Chemistry"]


def test_openalex_normalized_schema_requires_hashes() -> None:
    frame = pd.DataFrame({"doi_clean": ["10.1000/test"], "index": [0]})

    with pytest.raises(Exception):
        OpenAlexNormalizedSchema.validate(frame)
