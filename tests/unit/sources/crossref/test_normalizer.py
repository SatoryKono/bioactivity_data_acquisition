"""Crossref normalizer tests."""

from __future__ import annotations

from unittest.mock import patch

import bioetl.adapters.crossref as crossref_module
from bioetl.adapters._normalizer_helpers import get_bibliography_normalizers
from tests.unit.sources.crossref import CrossrefAdapterTestCase


class TestCrossrefNormalizer(CrossrefAdapterTestCase):
    """Validate Crossref normalization helpers."""

    def test_normalize_record(self) -> None:
        """Normalization enriches DOI, authors and year fields."""

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
                    "ORCID": "https://orcid.org/0000-0001-2345-6789",
                    "affiliation": [{"name": "Example Research Lab"}],
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
        self.assertIn("author_affiliations", normalized)
        self.assertIn("Example Research Lab", normalized["author_affiliations"])

    @patch("bioetl.adapters.crossref.normalize_common_bibliography", autospec=True)
    def test_common_helper_invoked(self, helper_mock) -> None:
        """The shared bibliography helper is used for normalization."""

        helper_mock.return_value = {
            "doi_clean": "10.1000/common",
            "title": "Common Title",
            "journal": "Common Journal",
            "authors": "Common Author",
        }

        record = {
            "DOI": "10.1000/common",
            "author": [{"ORCID": "https://orcid.org/0000-0000-0000-0000"}],
        }

        normalized = self.adapter.normalize_record(record)

        helper_mock.assert_called_once()
        _, kwargs = helper_mock.call_args
        self.assertEqual(kwargs["doi"], "DOI")
        self.assertEqual(kwargs["title"], "title")
        self.assertEqual(kwargs["journal"], ("container-title", "short-container-title"))
        self.assertEqual(kwargs["authors"], "author")
        self.assertIs(kwargs["authors_normalizer"], crossref_module.normalize_crossref_authors)
        self.assertTrue(callable(kwargs["journal_normalizer"]))

        self.assertEqual(normalized["doi_clean"], "10.1000/common")
        self.assertEqual(normalized["crossref_doi"], "10.1000/common")
        self.assertEqual(normalized["title"], "Common Title")

    def test_uses_shared_bibliography_normalizers(self) -> None:
        """Module-level normalizers come from the shared helper."""

        identifier, string = get_bibliography_normalizers()

        self.assertIs(crossref_module.NORMALIZER_ID, identifier)
        self.assertIs(crossref_module.NORMALIZER_STRING, string)
