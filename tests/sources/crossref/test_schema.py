"""Crossref schema coverage tests."""

from __future__ import annotations

import unittest

from bioetl.schemas.document import DocumentSchema


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
