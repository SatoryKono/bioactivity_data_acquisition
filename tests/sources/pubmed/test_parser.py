"""PubMed parser tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from tests.sources.pubmed import PubMedAdapterTestCase


class TestPubMedParser(PubMedAdapterTestCase):
    """Validate XML parsing helpers for PubMed."""

    @patch("bioetl.adapters.pubmed.ET.fromstring")
    def test_parse_xml(self, mock_et):
        """Test XML parsing into article dictionaries."""

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

    def test_normalize_date(self):
        """Test date normalization helper."""

        result = self.adapter._normalize_date(2023, "Mar", "15")
        self.assertEqual(result, "2023-03-15")

        result = self.adapter._normalize_date(2023, None, None)
        self.assertEqual(result, "2023-00-00")

    def test_month_to_int(self):
        """Month conversion handles abbreviations and defaults."""

        self.assertEqual(self.adapter._month_to_int("Mar"), 3)
        self.assertEqual(self.adapter._month_to_int("January"), 1)
        self.assertEqual(self.adapter._month_to_int("unknown"), 1)
