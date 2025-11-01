"""Tests for the PubMed WebEnv paginator."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from bioetl.sources.pubmed.pagination import WebEnvPaginator


@dataclass
class StubClient:
    """Stub UnifiedAPIClient recording requests."""

    esearch_response: Mapping[str, Any]
    efetch_responses: list[str]
    esearch_calls: list[dict[str, Any]] = field(default_factory=list)
    efetch_calls: list[dict[str, Any]] = field(default_factory=list)

    def request_json(self, path: str, params: Mapping[str, Any]) -> Mapping[str, Any]:
        assert path == "/esearch.fcgi"
        self.esearch_calls.append(dict(params))
        return self.esearch_response

    def request_text(self, path: str, params: Mapping[str, Any]) -> str:
        assert path == "/efetch.fcgi"
        self.efetch_calls.append(dict(params))
        if self.efetch_responses:
            return self.efetch_responses.pop(0)
        return "<PubmedArticleSet></PubmedArticleSet>"


def test_fetch_all_batches_returns_all_records() -> None:
    """Paginator should walk through the entire WebEnv history."""

    esearch_payload = {
        "esearchresult": {
            "webenv": "NCID_1_123", "querykey": "1", "count": "3", "idlist": ["1", "2", "3"]
        }
    }

    def build_payload(pmids: list[int]) -> str:
        articles = "".join(
            f"""
        <PubmedArticle>
            <MedlineCitation>
                <PMID>{pmid}</PMID>
                <Article>
                    <ArticleTitle>Title {pmid}</ArticleTitle>
                    <Abstract>
                        <AbstractText>Abstract {pmid}</AbstractText>
                    </Abstract>
                </Article>
            </MedlineCitation>
        </PubmedArticle>
        """  # noqa: D401 - inline formatting
            for pmid in pmids
        )
        return f"<PubmedArticleSet>{articles}</PubmedArticleSet>"

    efetch_payloads = [build_payload([1, 2]), build_payload([3])]

    client = StubClient(esearch_response=esearch_payload, efetch_responses=efetch_payloads.copy())
    paginator = WebEnvPaginator(client, batch_size=2)

    records = paginator.fetch_all({"term": "aspirin"})

    assert [record["pmid"] for record in records] == [1, 2, 3]
    assert len(client.esearch_calls) == 1
    assert len(client.efetch_calls) == 2

    first_call = client.efetch_calls[0]
    second_call = client.efetch_calls[1]

    assert first_call["retstart"] == 0
    assert first_call["retmax"] == 2
    assert second_call["retstart"] == 2
    assert second_call["retmax"] == 1
    assert first_call["retmode"] == "xml"
    assert first_call["rettype"] == "abstract"


def test_fetch_all_handles_missing_tokens() -> None:
    """Missing WebEnv or QueryKey should short-circuit pagination."""

    client = StubClient(esearch_response={}, efetch_responses=[])
    paginator = WebEnvPaginator(client)

    records = paginator.fetch_all({"term": "imatinib"})

    assert records == []
    assert client.efetch_calls == []


def test_fetch_all_respects_custom_fetch_params() -> None:
    """Custom fetch parameters should be forwarded to each request."""

    esearch_payload = {"WebEnv": "token", "QueryKey": "1", "Count": "1"}
    efetch_payloads = [
        """
        <PubmedArticleSet>
            <PubmedArticle>
                <MedlineCitation><PMID>42</PMID></MedlineCitation>
            </PubmedArticle>
        </PubmedArticleSet>
        """
    ]

    client = StubClient(esearch_response=esearch_payload, efetch_responses=efetch_payloads.copy())
    paginator = WebEnvPaginator(client, batch_size=50)

    records = paginator.fetch_all({"term": "2024"}, fetch_params={"rettype": "full"})

    assert len(records) == 1
    assert client.efetch_calls[0]["rettype"] == "full"
    assert client.efetch_calls[0]["retmode"] == "xml"
