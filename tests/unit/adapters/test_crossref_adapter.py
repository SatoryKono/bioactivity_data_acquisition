"""Unit tests for CrossrefAdapter."""

import unittest
from unittest.mock import patch

from bioetl.adapters.crossref import CrossrefAdapter

from tests.unit.adapters._mixins import AdapterTestMixin


class TestCrossrefAdapter(AdapterTestMixin, unittest.TestCase):
    """Test CrossrefAdapter."""

    ADAPTER_CLASS = CrossrefAdapter
    API_CONFIG_OVERRIDES = {
        "name": "crossref",
        "base_url": "https://api.crossref.org",
        "rate_limit_max_calls": 2,
    }
    ADAPTER_CONFIG_OVERRIDES = {
        "batch_size": 100,
        "workers": 2,
        "mailto": "test@example.com",
    }

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

    def test_fetch_by_ids_batches_using_class_default(self) -> None:
        """Batch helper respects ``DEFAULT_BATCH_SIZE`` fallback."""

        adapter = self.ADAPTER_CLASS(
            self.api_config,
            self.make_adapter_config(batch_size=0, mailto="test@example.com"),
        )

        total = adapter.DEFAULT_BATCH_SIZE * 2 + 5
        identifiers = [f"10.1234/test{i}" for i in range(total)]

        with patch.object(adapter, "_fetch_batch", return_value=[], autospec=True) as batch_mock:
            adapter.fetch_by_ids(identifiers)

        expected_calls = -(-total // adapter.DEFAULT_BATCH_SIZE)
        self.assertEqual(batch_mock.call_count, expected_calls)
        first_batch = batch_mock.call_args_list[0].args[1]
        self.assertEqual(len(first_batch), adapter.DEFAULT_BATCH_SIZE)
        last_batch = batch_mock.call_args_list[-1].args[1]
        remainder = total % adapter.DEFAULT_BATCH_SIZE
        expected_last = remainder or adapter.DEFAULT_BATCH_SIZE
        self.assertEqual(len(last_batch), expected_last)

