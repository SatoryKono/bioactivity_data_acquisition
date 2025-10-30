"""Unit tests for OpenAlexAdapter."""

import unittest
from unittest.mock import patch

import bioetl.adapters.openalex as openalex_module
from bioetl.adapters._normalizer_helpers import get_bibliography_normalizers
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

    @patch("bioetl.adapters.openalex.normalize_common_bibliography", autospec=True)
    def test_common_helper_invoked(self, helper_mock):
        """The shared bibliography helper is used for normalization."""

        helper_mock.return_value = {
            "doi_clean": "10.2000/common",
            "title": "OA Title",
            "journal": "OA Journal",
            "authors": "Author One",
        }

        record = {
            "id": "https://openalex.org/W1",
            "doi": "10.2000/common",
            "primary_location": {"source": {}},
            "authorships": [],
        }

        normalized = self.adapter.normalize_record(record)

        helper_mock.assert_called_once()
        args, kwargs = helper_mock.call_args
        self.assertIs(args[0], record)
        self.assertEqual(kwargs["doi"], "doi")
        self.assertEqual(kwargs["title"], "title")
        self.assertEqual(kwargs["authors"], "authorships")
        self.assertIn("journal", kwargs)
        self.assertTrue(callable(kwargs["journal"]))
        self.assertIn("journal_normalizer", kwargs)

        self.assertEqual(normalized["doi_clean"], "10.2000/common")
        self.assertEqual(normalized["openalex_doi"], "10.2000/common")
        self.assertEqual(normalized["title"], "OA Title")

    def test_uses_shared_bibliography_normalizers(self) -> None:
        """Module-level normalizers come from the shared helper."""

        identifier, string = get_bibliography_normalizers()

        self.assertIs(openalex_module.NORMALIZER_ID, identifier)
        self.assertIs(openalex_module.NORMALIZER_STRING, string)

    def test_fetch_by_ids_batches_using_class_default(self) -> None:
        """Batch helper falls back to ``DEFAULT_BATCH_SIZE``."""

        adapter = self.ADAPTER_CLASS(
            self.api_config,
            self.make_adapter_config(batch_size=0),
        )

        total = adapter.DEFAULT_BATCH_SIZE * 2 + 5
        identifiers = [f"10.1234/openalex{i}" for i in range(total)]

        with patch.object(adapter, "_fetch_batch", return_value=[], autospec=True) as batch_mock:
            adapter.fetch_by_ids(identifiers)

        expected_calls = -(-total // adapter.DEFAULT_BATCH_SIZE)
        self.assertEqual(batch_mock.call_count, expected_calls)
        self.assertEqual(len(batch_mock.call_args_list[0].args[1]), adapter.DEFAULT_BATCH_SIZE)
        remainder = total % adapter.DEFAULT_BATCH_SIZE
        expected_last = remainder or adapter.DEFAULT_BATCH_SIZE
        self.assertEqual(len(batch_mock.call_args_list[-1].args[1]), expected_last)

