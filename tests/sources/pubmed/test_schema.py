"""PubMed schema coverage tests."""

from __future__ import annotations

import unittest

from bioetl.schemas.document import DocumentSchema


class TestPubMedSchema(unittest.TestCase):
    """Ensure PubMed enrichment columns exist in the unified document schema."""

    def test_document_schema_includes_pubmed_columns(self) -> None:
        columns = set(DocumentSchema.get_column_order())
        expected = {
            "pubmed_id",
            "pubmed_pmid",
            "pubmed_doi",
            "pubmed_issn",
            "pubmed_mesh_descriptors",
        }

        self.assertTrue(expected.issubset(columns))
