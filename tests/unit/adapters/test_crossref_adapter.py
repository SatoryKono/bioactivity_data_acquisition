"""Unit tests for CrossrefAdapter."""

import unittest
from unittest.mock import MagicMock

from bioetl.adapters.base import AdapterConfig
from bioetl.adapters.crossref import CrossrefAdapter
from bioetl.core.api_client import APIConfig


class TestCrossrefAdapter(unittest.TestCase):
    """Test CrossrefAdapter."""

    def setUp(self):
        """Set up test fixtures."""
        api_config = APIConfig(
            name="crossref",
            base_url="https://api.crossref.org",
            rate_limit_max_calls=2,
            rate_limit_period=1.0,
        )
        adapter_config = AdapterConfig(
            enabled=True,
            batch_size=100,
            workers=2,
        )
        adapter_config.mailto = "test@example.com"

        self.adapter = CrossrefAdapter(api_config, adapter_config)

    def test_normalize_record(self):
        """Test record normalization."""
        record = {
            "DOI": "10.1371/journal.pone.0123456",
            "title": ["Test Article"],
            "container-title": ["PLoS ONE"],
            "published-print": {"date-parts": [[2023, 3, 15]]},
            "volume": "18",
            "issue": "3",
            "ISSN": ["1932-6203"],
            "author": [
                {
                    "given": "John",
                    "family": "Doe",
                    "ORCID": "https://orcid.org/0000-0001-2345-6789"
                }
            ],
        }

        normalized = self.adapter.normalize_record(record)
        self.assertIn("doi_clean", normalized)
        self.assertEqual(normalized["doi_clean"], "10.1371/journal.pone.0123456")
        self.assertIn("year", normalized)
        self.assertEqual(normalized["year"], 2023)
        self.assertIn("authors", normalized)
        self.assertIn("Doe, John", normalized["authors"])

