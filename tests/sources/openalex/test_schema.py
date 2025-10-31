"""OpenAlex schema coverage tests."""

from __future__ import annotations

import unittest

from bioetl.schemas.document import DocumentSchema


class TestOpenAlexSchema(unittest.TestCase):
    """Ensure OpenAlex specific columns are present in :class:`DocumentSchema`."""

    def test_document_schema_includes_openalex_columns(self) -> None:
        columns = set(DocumentSchema.get_column_order())
        expected = {
            "openalex_id",
            "openalex_doi",
            "openalex_pmid",
            "openalex_issn",
            "openalex_crossref_doc_type",
        }

        self.assertTrue(expected.issubset(columns))
