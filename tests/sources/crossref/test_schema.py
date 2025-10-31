"""Crossref schema coverage tests."""

from __future__ import annotations

import unittest

import pandas as pd

from bioetl.schemas.document import DocumentSchema
from bioetl.sources.crossref.schema import CrossrefNormalizedSchema, CrossrefRawSchema


class TestCrossrefSchema(unittest.TestCase):
    """Ensure Crossref columns are exposed by :class:`DocumentSchema`."""

    def test_document_schema_includes_crossref_columns(self) -> None:
        """The unified document schema must expose Crossref specific fields."""

        columns = set(DocumentSchema.get_column_order())
        expected = {
            "crossref_title",
            "crossref_authors",
            "crossref_doi",
            "crossref_doc_type",
            "openalex_crossref_doc_type",
            "crossref_subject",
        }

        self.assertTrue(expected.issubset(columns))


def _base_metadata() -> dict[str, list[object]]:
    return {
        "index": [0],
        "hash_row": ["0" * 64],
        "hash_business_key": ["f" * 64],
        "pipeline_version": ["1.0.0"],
        "run_id": ["run-001"],
        "source_system": ["crossref"],
        "chembl_release": ["33"],
        "extracted_at": ["2024-01-01T00:00:00Z"],
    }


def test_crossref_raw_schema_validates_payload() -> None:
    """The raw schema should accept hydrated Crossref messages."""

    df = pd.DataFrame(
        {
            **_base_metadata(),
            "DOI": ["10.1000/raw"],
            "title": [["Raw Title"]],
            "author": [[{"given": "Ada", "family": "Lovelace"}]],
            "publisher": ["Crossref"],
            "subject": [["Chemistry"]],
            "type": ["journal-article"],
            "container_title": [["Journal"]],
            "short_container_title": [["Jnl"]],
            "published_print": [{"date-parts": [[2024, 5, 4]]}],
            "published_online": [None],
            "issued": [None],
            "created": [None],
            "volume": ["12"],
            "issue": ["3"],
            "page": ["123-130"],
            "ISSN": [["1234-5678"]],
            "issn_type": [[{"type": "print", "value": "1234-5678"}]],
        }
    )

    validated = CrossrefRawSchema.validate(df)
    assert not validated.empty


def test_crossref_normalized_schema_validates_dataframe() -> None:
    """Normalized schema should accept canonical Crossref enrichment rows."""

    df = pd.DataFrame(
        {
            **_base_metadata(),
            "doi_clean": ["10.1000/normalized"],
            "crossref_doi": ["10.1000/normalized"],
            "title": ["Normalized Title"],
            "journal": ["Journal"],
            "authors": ["Doe, John"],
            "year": pd.Series([2024], dtype="Int64"),
            "month": pd.Series([5], dtype="Int64"),
            "day": pd.Series([4], dtype="Int64"),
            "volume": ["12"],
            "issue": ["3"],
            "first_page": ["123"],
            "issn_print": ["1234-5678"],
            "issn_electronic": ["8765-4321"],
            "orcid": ["0000-0001-2345-6789"],
            "publisher": ["Crossref"],
            "subject": ["Chemistry"],
            "doc_type": ["journal-article"],
        }
    )

    validated = CrossrefNormalizedSchema.validate(df)
    assert not validated.empty
