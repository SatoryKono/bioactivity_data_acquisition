"""PubMed pipeline end-to-end tests."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from tests.unit.sources.pubmed import PubMedAdapterTestCase


class TestPubMedPipelineE2E(PubMedAdapterTestCase):
    """Exercise the fetch → parse → normalize flow with mocked IO."""

    def test_fetch_parse_normalize_cycle(self) -> None:
        """XML payloads fetched via the adapter are parsed and normalized."""

        adapter = self.adapter
        xml_payload = """
        <PubmedArticleSet>
            <PubmedArticle>
                <MedlineCitation>
                    <PMID>12345678</PMID>
                    <Article>
                        <ArticleTitle>Integration Test</ArticleTitle>
                    </Article>
                </MedlineCitation>
            </PubmedArticle>
        </PubmedArticleSet>
        """

        with patch.object(adapter.api_client, "request_text", return_value=xml_payload):
            records = adapter.fetch_by_ids(["12345678"])

        self.assertEqual(len(records), 1)

        normalized = pd.DataFrame([adapter.normalize_record(record) for record in records])
        self.assertIn("pubmed_id", normalized.columns)
        self.assertEqual(normalized.loc[0, "pubmed_id"], "12345678")
