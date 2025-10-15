from __future__ import annotations

import pytest
import responses

from bioactivity.clients import (
    ApiClientError,
    ChEMBLClient,
    CrossrefClient,
    OpenAlexClient,
    PubMedClient,
    RateLimitConfig,
    RateLimiter,
    RateLimitError,
    SemanticScholarClient,
)
from bioactivity.config import APIClientConfig, RateLimitSettings, RetrySettings


def make_config(name: str, base_url: str, **kwargs: object) -> APIClientConfig:
    data: dict[str, object] = {
        "endpoint": "",
        "headers": {},
        "params": {},
        "timeout": 30.0,
        "retries": RetrySettings(),
    }
    data.update(kwargs)
    return APIClientConfig(name=name, base_url=base_url, **data)


@responses.activate
def test_chembl_fetch_by_doc_id_success() -> None:
    client = ChEMBLClient(make_config("chembl", "https://www.ebi.ac.uk/chembl/api/data"))
    responses.add(
        responses.GET,
        "https://www.ebi.ac.uk/chembl/api/data/document/CHEMBL123",
        json={
            "document_chembl_id": "CHEMBL123",
            "pubmed_id": "999",
            "doi": "10.1000/test",
            "title": "Test title",
            "abstract": "Body",
        },
    )

    data = client.fetch_by_doc_id("CHEMBL123")

    assert data == {
        "source": "chembl",
        "document_chembl_id": "CHEMBL123",
        "pubmed_id": "999",
        "doi": "10.1000/test",
        "title": "Test title",
        "abstract": "Body",
    }


@responses.activate
def test_chembl_fetch_by_doc_id_http_error() -> None:
    client = ChEMBLClient(make_config("chembl", "https://www.ebi.ac.uk/chembl/api/data"))
    responses.add(
        responses.GET,
        "https://www.ebi.ac.uk/chembl/api/data/document/CHEMBL404",
        status=404,
        json={"error": "not found"},
    )

    with pytest.raises(ApiClientError):
        client.fetch_by_doc_id("CHEMBL404")


@responses.activate
def test_rate_limit_enforced_before_request() -> None:
    limiter = RateLimiter(RateLimitConfig(max_calls=1, period=60))
    cfg = make_config(
        "chembl",
        "https://www.ebi.ac.uk/chembl/api/data",
        rate_limit=RateLimitSettings(max_calls=1, period=60),
    )
    client = ChEMBLClient(cfg, rate_limiter=limiter)
    responses.add(
        responses.GET,
        "https://www.ebi.ac.uk/chembl/api/data/document/CHEMBL001",
        json={"document_chembl_id": "CHEMBL001"},
    )

    client.fetch_by_doc_id("CHEMBL001")
    with pytest.raises(RateLimitError):
        client.fetch_by_doc_id("CHEMBL001")


@responses.activate
def test_pubmed_fetch_by_pmid_parses_record() -> None:
    client = PubMedClient(make_config("pubmed", "https://api.ncbi.nlm.nih.gov/lit/ctxp/v1/pubmed"))
    responses.add(
        responses.GET,
        "https://api.ncbi.nlm.nih.gov/lit/ctxp/v1/pubmed",
        json={
            "result": {
                "uids": ["12345"],
                "12345": {
                    "PMID": "12345",
                    "title": "Article",
                    "abstract": "Summary",
                    "doi": "10.1000/pubmed",
                    "authors": [
                        {"lastName": "Doe", "foreName": "Jane"},
                        "Smith",
                    ],
                },
            }
        },
    )

    result = client.fetch_by_pmid("12345")

    assert result == {
        "source": "pubmed",
        "pmid": "12345",
        "title": "Article",
        "abstract": "Summary",
        "doi": "10.1000/pubmed",
        "authors": ["Jane Doe", "Smith"],
    }


@responses.activate
def test_pubmed_fetch_by_pmids_batch() -> None:
    client = PubMedClient(make_config("pubmed", "https://api.ncbi.nlm.nih.gov/lit/ctxp/v1/pubmed"))
    responses.add(
        responses.POST,
        "https://api.ncbi.nlm.nih.gov/lit/ctxp/v1/pubmed/batch",
        json={
            "records": [
                {
                    "pmid": "111",
                    "title": "First",
                },
                {
                    "pmid": "222",
                    "title": "Second",
                },
            ]
        },
    )

    result = client.fetch_by_pmids(["111", "222", "333"])

    assert result == {
        "111": {"source": "pubmed", "pmid": "111", "title": "First"},
        "222": {"source": "pubmed", "pmid": "222", "title": "Second"},
    }


@responses.activate
def test_semantic_scholar_batch_parsing() -> None:
    client = SemanticScholarClient(
        make_config("semantic_scholar", "https://api.semanticscholar.org/graph/v1/paper")
    )
    responses.add(
        responses.POST,
        "https://api.semanticscholar.org/graph/v1/paper/batch",
        json={
            "data": [
                {
                    "externalIds": {"PubMed": "123", "DOI": "10.1/123"},
                    "title": "Paper",
                    "abstract": "Abstract",
                    "year": 2020,
                    "authors": [{"name": "Author One"}],
                }
            ]
        },
    )

    result = client.fetch_by_pmids(["123"])

    assert result == {
        "123": {
            "source": "semantic_scholar",
            "pmid": "123",
            "doi": "10.1/123",
            "title": "Paper",
            "abstract": "Abstract",
            "year": 2020,
            "authors": ["Author One"],
        }
    }


@responses.activate
def test_crossref_fallback_to_search() -> None:
    client = CrossrefClient(make_config("crossref", "https://api.crossref.org/works"))
    responses.add(
        responses.GET,
        "https://api.crossref.org/works/10.1000%2Ffallback",
        status=404,
        json={"status": "not-found"},
    )
    responses.add(
        responses.GET,
        "https://api.crossref.org/works",
        match=[responses.matchers.query_param_matcher({"query.bibliographic": "10.1000/fallback"})],
        json={
            "message": {
                "items": [
                    {"DOI": "10.1000/fallback", "title": ["Crossref Title"], "pub-med-id": "999"}
                ]
            }
        },
    )

    result = client.fetch_by_doi("10.1000/fallback")

    assert result == {
        "source": "crossref",
        "doi": "10.1000/fallback",
        "title": "Crossref Title",
        "pmid": "999",
    }


@responses.activate
def test_openalex_pmid_fallback_to_search() -> None:
    client = OpenAlexClient(make_config("openalex", "https://api.openalex.org/works"))
    responses.add(
        responses.GET,
        "https://api.openalex.org/works",
        match=[responses.matchers.query_param_matcher({"filter": "pmid:101"})],
        json={"results": []},
    )
    responses.add(
        responses.GET,
        "https://api.openalex.org/works",
        match=[responses.matchers.query_param_matcher({"search": "101"})],
        json={
            "results": [
                {
                    "id": "https://openalex.org/W1",
                    "display_name": "OpenAlex Paper",
                    "ids": {"pmid": "PMID:101", "doi": "https://doi.org/10.1000/open"},
                }
            ]
        },
    )

    result = client.fetch_by_pmid("101")

    assert result == {
        "source": "openalex",
        "openalex_id": "https://openalex.org/W1",
        "pmid": "PMID:101",
        "doi": "https://doi.org/10.1000/open",
        "title": "OpenAlex Paper",
    }


@responses.activate
def test_openalex_fetch_by_doi_fallback_filter() -> None:
    client = OpenAlexClient(make_config("openalex", "https://api.openalex.org/works"))
    responses.add(
        responses.GET,
        "https://api.openalex.org/works/https://doi.org/10.1000/primary",
        status=404,
        json={"status": "not-found"},
    )
    responses.add(
        responses.GET,
        "https://api.openalex.org/works",
        match=[responses.matchers.query_param_matcher({"filter": "doi:10.1000/primary"})],
        json={"results": [{"id": "https://openalex.org/W2", "display_name": "Fallback"}]},
    )

    result = client.fetch_by_doi("10.1000/primary")

    assert result == {
        "source": "openalex",
        "openalex_id": "https://openalex.org/W2",
        "title": "Fallback",
    }


@responses.activate
def test_semantic_scholar_single_fetch() -> None:
    client = SemanticScholarClient(
        make_config("semantic_scholar", "https://api.semanticscholar.org/graph/v1/paper")
    )
    responses.add(
        responses.GET,
        "https://api.semanticscholar.org/graph/v1/paper/PMID:777",
        match=[responses.matchers.query_param_matcher({"fields": ",".join(client._DEFAULT_FIELDS)})],
        json={
            "externalIds": {"PubMed": "PMID:777"},
            "title": "Single",
        },
    )

    result = client.fetch_by_pmid("777")

    assert result == {"source": "semantic_scholar", "pmid": "777", "title": "Single"}


@responses.activate
def test_crossref_fetch_by_pmid_fallback_when_no_items() -> None:
    client = CrossrefClient(make_config("crossref", "https://api.crossref.org/works"))
    responses.add(
        responses.GET,
        "https://api.crossref.org/works",
        match=[responses.matchers.query_param_matcher({"filter": "pmid:555"})],
        json={"message": {"items": []}},
    )
    responses.add(
        responses.GET,
        "https://api.crossref.org/works",
        match=[responses.matchers.query_param_matcher({"query": "555"})],
        json={"message": {"items": [{"DOI": "10.555/example", "title": ["Example"]}]}},
    )

    result = client.fetch_by_pmid("555")

    assert result == {
        "source": "crossref",
        "doi": "10.555/example",
        "title": "Example",
    }