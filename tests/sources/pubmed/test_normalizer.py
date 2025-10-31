"""PubMed normalizer tests."""

from __future__ import annotations

from unittest.mock import patch

import bioetl.adapters.pubmed as pubmed_module
from bioetl.adapters._normalizer_helpers import get_bibliography_normalizers
from tests.sources.pubmed import PubMedAdapterTestCase


class TestPubMedNormalizer(PubMedAdapterTestCase):
    """Validate normalization logic for PubMed adapter."""

    def test_normalize_record(self):
        """Test record normalization."""

        record = {
            "pmid": "12345678",
            "title": "Test Article",
            "abstract": "Test abstract",
            "journal": "Test Journal",
            "year": 2023,
            "authors": "Smith, John",
        }

        normalized = self.adapter.normalize_record(record)
        self.assertIn("pubmed_id", normalized)
        self.assertEqual(normalized["pubmed_id"], "12345678")

    @patch("bioetl.adapters.pubmed.normalize_common_bibliography", autospec=True)
    def test_common_helper_invoked(self, helper_mock):
        """The shared bibliography helper is used for normalization."""

        helper_mock.return_value = {
            "doi_clean": "10.3000/common",
            "title": "PM Title",
            "journal": "PM Journal",
            "authors": "Author One",
        }

        record = {
            "pmid": "12345",
            "doi": "10.3000/common",
            "journal": "Ignored Journal",
            "authors": "A, B",
        }

        normalized = self.adapter.normalize_record(record)

        helper_mock.assert_called_once()
        args, kwargs = helper_mock.call_args
        self.assertIs(args[0], record)
        self.assertEqual(kwargs["doi"], "doi")
        self.assertEqual(kwargs["title"], "title")
        self.assertEqual(kwargs["journal"], "journal")
        self.assertEqual(kwargs["authors"], "authors")
        self.assertIn("journal_normalizer", kwargs)

        self.assertEqual(normalized["doi_clean"], "10.3000/common")
        self.assertEqual(normalized["pubmed_doi"], "10.3000/common")
        self.assertEqual(normalized["title"], "PM Title")

    def test_uses_shared_bibliography_normalizers(self) -> None:
        """Module-level normalizers come from the shared helper."""

        identifier, string = get_bibliography_normalizers()

        self.assertIs(pubmed_module.NORMALIZER_ID, identifier)
        self.assertIs(pubmed_module.NORMALIZER_STRING, string)
