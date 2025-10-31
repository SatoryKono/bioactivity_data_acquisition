"""Semantic Scholar schema coverage tests."""

from __future__ import annotations

import unittest

import pandas as pd
from pandera.errors import SchemaError

from bioetl.schemas.document import DocumentSchema
from bioetl.sources.semantic_scholar.schema import (
    SemanticScholarNormalizedSchema,
    SemanticScholarRawSchema,
)


class TestSemanticScholarSchema(unittest.TestCase):
    """Ensure Semantic Scholar columns are present on the document schema."""

    def test_document_schema_includes_semantic_columns(self) -> None:
        columns = set(DocumentSchema.get_column_order())
        expected = {
            "semantic_scholar_paper_id",
            "semantic_scholar_citation_count",
            "semantic_scholar_influential_citations",
            "semantic_scholar_reference_count",
        }

        self.assertTrue(expected.issubset(columns))


class TestSemanticScholarRawSchema(unittest.TestCase):
    """Validate the Pandera schema for raw API payloads."""

    def test_raw_schema_accepts_valid_payload(self) -> None:
        df = pd.DataFrame(
            {
                "paperId": ["hash-1"],
                "title": ["A sample paper"],
                "abstract": ["Exploring pagination."],
                "venue": ["Journal of Testing"],
                "year": [2024],
                "publicationDate": ["2024-01-01"],
                "externalIds": [[{"DOI": "10.1000/example"}]],
                "authors": [[{"name": "Author One"}]],
                "citationCount": [10],
                "influentialCitationCount": [2],
                "referenceCount": [5],
                "isOpenAccess": [True],
                "publicationTypes": [["JournalArticle"]],
                "fieldsOfStudy": [["Medicine"]],
            }
        )

        validated = SemanticScholarRawSchema.validate(df)
        self.assertEqual(len(validated), 1)

    def test_raw_schema_rejects_missing_identifier(self) -> None:
        df = pd.DataFrame(
            {
                "paperId": [None],
                "title": ["Invalid payload"],
                "abstract": [pd.NA],
                "venue": [pd.NA],
                "year": [pd.NA],
                "publicationDate": [pd.NA],
                "externalIds": [pd.NA],
                "authors": [pd.NA],
                "citationCount": [pd.NA],
                "influentialCitationCount": [pd.NA],
                "referenceCount": [pd.NA],
                "isOpenAccess": [pd.NA],
                "publicationTypes": [pd.NA],
                "fieldsOfStudy": [pd.NA],
            }
        )

        with self.assertRaises(SchemaError):
            SemanticScholarRawSchema.validate(df)


class TestSemanticScholarNormalizedSchema(unittest.TestCase):
    """Validate the normalized enrichment schema."""

    def test_normalized_schema_accepts_expected_columns(self) -> None:
        df = pd.DataFrame(
            {
                "doi_clean": ["10.1000/example"],
                "paper_id": ["paper-1"],
                "pubmed_id": ["12345"],
                "title": ["Normalised"],
                "journal": ["Journal"],
                "authors": ["Author One"],
                "abstract": ["Text"],
                "year": [2024],
                "publication_date": ["2024-01-01"],
                "citation_count": [10],
                "influential_citations": [2],
                "reference_count": [5],
                "is_oa": [True],
                "publication_types": [["journalarticle"]],
                "doc_type": ["journalarticle"],
                "fields_of_study": [["Biology"]],
            }
        )

        validated = SemanticScholarNormalizedSchema.validate(df)
        self.assertEqual(len(validated), 1)

    def test_normalized_schema_rejects_invalid_metric(self) -> None:
        df = pd.DataFrame(
            {
                "doi_clean": ["10.1000/example"],
                "paper_id": ["paper-1"],
                "pubmed_id": ["12345"],
                "title": ["Normalised"],
                "journal": ["Journal"],
                "authors": ["Author One"],
                "abstract": ["Text"],
                "year": [2024],
                "publication_date": ["2024-01-01"],
                "citation_count": ["ten"],
                "influential_citations": [2],
                "reference_count": [5],
                "is_oa": [True],
                "publication_types": [["journalarticle"]],
                "doc_type": ["journalarticle"],
                "fields_of_study": [["Biology"]],
            }
        )

        with self.assertRaises(SchemaError):
            SemanticScholarNormalizedSchema.validate(df)
