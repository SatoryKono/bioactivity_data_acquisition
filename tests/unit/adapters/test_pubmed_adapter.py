"""Unit tests for PubMedAdapter."""

import unittest
from unittest.mock import MagicMock, patch

from bioetl.adapters.pubmed import PubMedAdapter

from tests.unit.adapters._mixins import AdapterTestMixin


class TestPubMedAdapter(AdapterTestMixin, unittest.TestCase):
    """Test PubMedAdapter."""

    ADAPTER_CLASS = PubMedAdapter
    API_CONFIG_OVERRIDES = {
        "name": "pubmed",
        "base_url": "https://eutils.ncbi.nlm.nih.gov/entrez/eutils",
        "rate_limit_max_calls": 3,
    }
    ADAPTER_CONFIG_OVERRIDES = {
        "batch_size": 200,
        "workers": 1,
        "email": "test@example.com",
        "api_key": "test_key",
    }

    @patch("bioetl.adapters.pubmed.ET.fromstring")
    def test_parse_xml(self, mock_et):
        """Test XML parsing."""
        # Mock XML content
        xml_content = """
        <PubmedArticleSet>
            <PubmedArticle>
                <MedlineCitation>
                    <PMID>12345678</PMID>
                    <Article>
                        <ArticleTitle>Test Article</ArticleTitle>
                        <Abstract>
                            <AbstractText>Test abstract</AbstractText>
                        </Abstract>
                    </Article>
                </MedlineCitation>
            </PubmedArticle>
        </PubmedArticleSet>
        """

        # Mock root element
        mock_root = MagicMock()
        mock_article = MagicMock()
        mock_pmid = MagicMock(text="12345678")
        mock_title = MagicMock(text="Test Article")

        mock_article.findall.return_value = []
        mock_root.findall.return_value = [mock_article]

        mock_root.find.return_value = mock_pmid
        mock_article.find.side_effect = lambda x: mock_title if x == ".//PMID" else None

        mock_et.return_value = mock_root

        result = self.adapter._parse_xml(xml_content)
        self.assertIsInstance(result, list)

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

    def test_normalize_date(self):
        """Test date normalization."""
        result = self.adapter._normalize_date(2023, "Mar", "15")
        self.assertEqual(result, "2023-03-15")

        result = self.adapter._normalize_date(2023, None, None)
        self.assertEqual(result, "2023-00-00")

    def test_month_to_int(self):
        """Test month conversion."""
        self.assertEqual(self.adapter._month_to_int("Mar"), 3)
        self.assertEqual(self.adapter._month_to_int("January"), 1)
        self.assertEqual(self.adapter._month_to_int("unknown"), 1)

