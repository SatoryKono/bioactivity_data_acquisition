"""Unit tests for OpenAlexAdapter."""

import unittest

from bioetl.adapters.openalex import OpenAlexAdapter

from tests.unit.adapters._mixins import AdapterTestMixin


class TestOpenAlexAdapter(AdapterTestMixin, unittest.TestCase):
    """Test OpenAlexAdapter."""

    ADAPTER_CLASS = OpenAlexAdapter
    API_CONFIG_OVERRIDES = {
        "name": "openalex",
        "base_url": "https://api.openalex.org",
        "rate_limit_max_calls": 10,
    }
    ADAPTER_CONFIG_OVERRIDES = {
        "batch_size": 100,
        "workers": 4,
    }

    def test_normalize_record(self):
        """Test record normalization."""
        record = {
            "id": "https://openalex.org/W123456789",
            "doi": "https://doi.org/10.1371/journal.pone.0123456",
            "title": "Test Article",
            "publication_date": "2023-03-15",
            "publication_year": 2023,
            "type": "article",
            "language": "en",
            "open_access": {
                "is_oa": True,
                "oa_status": "gold",
                "oa_url": "https://example.com/article"
            },
            "concepts": [
                {"display_name": "Medicine", "score": 0.9},
                {"display_name": "Biology", "score": 0.8},
                {"display_name": "Chemistry", "score": 0.7},
            ],
            "primary_location": {
                "source": {
                    "display_name": "PLoS ONE",
                    "name": "PLoS ONE"
                },
                "landing_page_url": "https://journals.plos.org/plosone/article"
            }
        }

        normalized = self.adapter.normalize_record(record)
        self.assertIn("openalex_id", normalized)
        self.assertEqual(normalized["openalex_id"], "W123456789")
        self.assertIn("is_oa", normalized)
        self.assertTrue(normalized["is_oa"])
        self.assertIn("concepts_top3", normalized)
        self.assertEqual(len(normalized["concepts_top3"]), 3)

