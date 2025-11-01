from __future__ import annotations

from unittest.mock import call, patch

import pandas as pd

from tests.sources.pubmed import PubMedAdapterTestCase


class TestPubMedFallback(PubMedAdapterTestCase):
    """Verify PubMed adapter fallbacks for DOI and title lookups."""

    def test_process_identifiers_resolves_doi_via_esearch(self) -> None:
        adapter = self.adapter

        xml_payload = (
            "<PubmedArticleSet>"
            "<PubmedArticle><MedlineCitation><PMID>111</PMID></MedlineCitation>"
            "<Article><ArticleTitle>Fallback DOI</ArticleTitle></Article>"
            "</PubmedArticle></PubmedArticleSet>"
        )
        esearch_response = {"esearchresult": {"idlist": ["111"]}}

        with (
            patch.object(adapter.api_client, "request_json", return_value=esearch_response) as json_mock,
            patch.object(adapter.api_client, "request_text", return_value=xml_payload) as text_mock,
        ):
            frame = adapter.process_identifiers(pmids=[], dois=["10.1234/test"], titles=[])

        assert not frame.empty
        assert frame["pmid"].astype(str).tolist() == ["111"]
        assert json_mock.call_count == 1
        params = json_mock.call_args.kwargs["params"]
        assert "term" in params and "10.1234/test" in params["term"]
        text_mock.assert_called_once()

    def test_process_identifiers_uses_title_when_doi_missing(self) -> None:
        adapter = self.adapter

        xml_payload = (
            "<PubmedArticleSet>"
            "<PubmedArticle><MedlineCitation><PMID>222</PMID></MedlineCitation>"
            "<Article><ArticleTitle>Fallback Title</ArticleTitle></Article>"
            "</PubmedArticle></PubmedArticleSet>"
        )
        esearch_empty = {"esearchresult": {"idlist": []}}
        esearch_success = {"esearchresult": {"idlist": ["222"]}}

        with (
            patch.object(
                adapter.api_client,
                "request_json",
                side_effect=[esearch_empty, esearch_success],
            ) as json_mock,
            patch.object(adapter.api_client, "request_text", return_value=xml_payload) as text_mock,
        ):
            frame = adapter.process_identifiers(
                pmids=[],
                dois=["10.9999/missing"],
                titles=["Fallback Title"],
            )

        assert not frame.empty
        assert frame["pmid"].astype(str).tolist() == ["222"]
        assert json_mock.call_count == 2
        terms = [call.kwargs["params"]["term"] for call in json_mock.call_args_list]
        assert any("AID" in term for term in terms)
        assert any("Title" in term for term in terms)
        text_mock.assert_called_once()
