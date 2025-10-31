"""Semantic Scholar normalizer tests."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd

import bioetl.adapters.semantic_scholar as semantic_module
from bioetl.adapters._normalizer_helpers import get_bibliography_normalizers
from tests.sources.semantic_scholar import SemanticScholarAdapterTestCase


class TestSemanticScholarNormalizer(SemanticScholarAdapterTestCase):
    """Validate normalization logic for Semantic Scholar."""

    def test_normalize_record(self):
        """Test record normalization."""

        record = {
            "paperId": "1234567890abcdef",
            "externalIds": {
                "DOI": "10.1371/journal.pone.0123456",
                "PubMed": "12345678",
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
            "fieldsOfStudy": ["Medicine", "Biology"],
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

    @patch("bioetl.adapters.semantic_scholar.normalize_common_bibliography", autospec=True)
    def test_common_helper_invoked(self, helper_mock):
        """The shared bibliography helper is used for normalization."""

        helper_mock.return_value = {
            "doi_clean": "10.4000/common",
            "title": "SS Title",
            "journal": "SS Journal",
            "authors": "Author One",
        }

        record = {
            "paperId": "id1",
            "externalIds": {},
            "authors": [],
        }

        normalized = self.adapter.normalize_record(record)

        helper_mock.assert_called_once()
        args, kwargs = helper_mock.call_args
        self.assertIs(args[0], record)
        self.assertTrue(callable(kwargs["doi"]))
        self.assertEqual(kwargs["title"], "title")
        self.assertEqual(kwargs["journal"], "venue")
        self.assertEqual(kwargs["authors"], "authors")
        self.assertIn("journal_normalizer", kwargs)

        self.assertEqual(normalized["doi_clean"], "10.4000/common")
        self.assertEqual(normalized["title"], "SS Title")
        self.assertEqual(normalized["_title_for_join"], "SS Title")

    def test_uses_shared_bibliography_normalizers(self) -> None:
        """Module-level normalizers come from the shared helper."""

        identifier, string = get_bibliography_normalizers()

        self.assertIs(semantic_module.NORMALIZER_ID, identifier)
        self.assertIs(semantic_module.NORMALIZER_STRING, string)

    def test_process_helpers_delegate(self) -> None:
        """``process`` utilities reuse the shared collection helper."""

        adapter = self.adapter

        with patch.object(
            semantic_module.SemanticScholarAdapter,
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

    def test_process_titles_helpers_delegate(self) -> None:
        adapter = self.adapter

        with patch.object(
            semantic_module.SemanticScholarAdapter,
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
