"""Unit tests for SemanticScholarAdapter."""

import unittest
from unittest.mock import patch

import pandas as pd

from bioetl.adapters.semantic_scholar import SemanticScholarAdapter

from tests.unit.adapters._mixins import AdapterTestMixin


class TestSemanticScholarAdapter(AdapterTestMixin, unittest.TestCase):
    """Test SemanticScholarAdapter."""

    ADAPTER_CLASS = SemanticScholarAdapter
    API_CONFIG_OVERRIDES = {
        "name": "semantic_scholar",
        "base_url": "https://api.semanticscholar.org/graph/v1",
        "rate_limit_period": 1.25,
    }
    ADAPTER_CONFIG_OVERRIDES = {
        "batch_size": 50,
        "workers": 1,
        "api_key": "test_key",
    }

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

    def test_fetch_by_ids_batches_using_class_default(self) -> None:
        """Batch helper respects ``DEFAULT_BATCH_SIZE`` fallback."""

        adapter = self.ADAPTER_CLASS(
            self.api_config,
            self.make_adapter_config(batch_size=0, api_key="test_key"),
        )

        total = adapter.DEFAULT_BATCH_SIZE * 2 + 5
        identifiers = [f"DOI:{i}" for i in range(total)]

        with patch.object(adapter, "_fetch_batch", return_value=[], autospec=True) as batch_mock:
            adapter.fetch_by_ids(identifiers)

        expected_calls = -(-total // adapter.DEFAULT_BATCH_SIZE)
        self.assertEqual(batch_mock.call_count, expected_calls)
        self.assertEqual(len(batch_mock.call_args_list[0].args[1]), adapter.DEFAULT_BATCH_SIZE)
        remainder = total % adapter.DEFAULT_BATCH_SIZE
        expected_last = remainder or adapter.DEFAULT_BATCH_SIZE
        self.assertEqual(len(batch_mock.call_args_list[-1].args[1]), expected_last)

    def test_process_uses_shared_helper(self) -> None:
        """``process`` delegates to :meth:`_process_collection`."""

        adapter = self.adapter

        with patch.object(
            SemanticScholarAdapter,
            "_process_collection",
            autospec=True,
            return_value=pd.DataFrame(),
        ) as helper_mock:
            result = adapter.process(["id-1", "id-2"])

        self.assertIsInstance(result, pd.DataFrame)
        helper_mock.assert_called_once()
        args, kwargs = helper_mock.call_args
        self.assertIs(args[0], adapter)
        self.assertEqual(list(args[1]), ["id-1", "id-2"])
        self.assertIs(args[2], adapter.fetch_by_ids)
        self.assertEqual(kwargs["start_event"], "starting_fetch")
        self.assertEqual(kwargs["no_items_event"], "no_ids_provided")
        self.assertEqual(kwargs["empty_event"], "no_records_fetched")

    def test_process_titles_uses_shared_helper(self) -> None:
        """``process_titles`` delegates to :meth:`_process_collection`."""

        adapter = self.adapter

        with patch.object(
            SemanticScholarAdapter,
            "_process_collection",
            autospec=True,
            return_value=pd.DataFrame(),
        ) as helper_mock:
            result = adapter.process_titles(["A title"])

        self.assertIsInstance(result, pd.DataFrame)
        helper_mock.assert_called_once()
        args, kwargs = helper_mock.call_args
        self.assertIs(args[0], adapter)
        self.assertEqual(list(args[1]), ["A title"])
        self.assertIs(args[2], adapter.fetch_by_titles)
        self.assertEqual(kwargs["start_event"], "starting_fetch_by_titles")
        self.assertEqual(kwargs["no_items_event"], "no_titles_provided")
        self.assertEqual(kwargs["empty_event"], "no_records_fetched")

