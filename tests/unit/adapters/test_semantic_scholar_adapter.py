"""Unit tests for SemanticScholarAdapter."""

import unittest

from bioetl.adapters.base import AdapterConfig
from bioetl.adapters.semantic_scholar import SemanticScholarAdapter
from bioetl.core.api_client import APIConfig


class TestSemanticScholarAdapter(unittest.TestCase):
    """Test SemanticScholarAdapter."""

    def setUp(self):
        """Set up test fixtures."""
        api_config = APIConfig(
            name="semantic_scholar",
            base_url="https://api.semanticscholar.org/graph/v1",
            rate_limit_max_calls=1,
            rate_limit_period=1.25,
        )
        adapter_config = AdapterConfig(
            enabled=True,
            batch_size=50,
            workers=1,
        )
        adapter_config.api_key = "test_key"

        self.adapter = SemanticScholarAdapter(api_config, adapter_config)

    def test_format_paper_id(self):
        """Test paper ID formatting."""
        self.assertEqual(self.adapter._format_paper_id("10.1371/journal.pone.0123456"), "10.1371/journal.pone.0123456")
        self.assertEqual(self.adapter._format_paper_id("12345678"), "PMID:12345678")
        self.assertEqual(self.adapter._format_paper_id("arXiv:1234.5678"), "arXiv:1234.5678")

    def test_normalize_record(self):
        """Test record normalization."""
        record = {
            "paperId": "1234567890abcdef",
            "externalIds": {
                "DOI": "10.1371/journal.pone.0123456",
                "PubMed": "12345678"
            },
            "title": "Test Article",
            "abstract": "Test abstract",
            "venue": "PLoS ONE",
            "year": 2023,
            "publicationDate": "2023-03-15",
            "citationCount": 120,
            "influentialCitationCount": 5,
            "referenceCount": 45,
            "isOpenAccess": True,
            "publicationTypes": ["JournalArticle"],
            "fieldsOfStudy": ["Medicine", "Biology"]
        }

        normalized = self.adapter.normalize_record(record)
        self.assertIn("doi_clean", normalized)
        self.assertEqual(normalized["doi_clean"], "10.1371/journal.pone.0123456")
        self.assertIn("citation_count", normalized)
        self.assertEqual(normalized["citation_count"], 120)
        self.assertIn("influential_citations", normalized)
        self.assertEqual(normalized["influential_citations"], 5)
        self.assertIn("fields_of_study", normalized)
        self.assertEqual(len(normalized["fields_of_study"]), 2)

