"""Semantic Scholar schema coverage tests."""

from __future__ import annotations

import unittest

from bioetl.schemas.document import DocumentSchema


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
