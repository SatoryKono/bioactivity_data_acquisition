"""Tests for the PubMed request builder."""

from bioetl.adapters.base import AdapterConfig
from bioetl.core.api_client import APIConfig
from bioetl.sources.pubmed.request import PubMedRequestBuilder


def test_efetch_includes_etiquette_parameters() -> None:
    """EFetch requests should embed tool/email/api key parameters."""

    api_config = APIConfig(name="pubmed", base_url="https://eutils.ncbi.nlm.nih.gov/entrez/eutils")
    adapter_config = AdapterConfig(tool="bioetl", email="labs@example.org", api_key="secret")
    builder = PubMedRequestBuilder(api_config, adapter_config)

    request = builder.efetch(["12345", "67890"])

    assert request.url.endswith("/efetch.fcgi")
    assert request.params["tool"] == "bioetl"
    assert request.params["email"] == "labs@example.org"
    assert request.params["api_key"] == "secret"
    assert list(request.params.keys()) == [
        "api_key",
        "db",
        "email",
        "id",
        "retmode",
        "rettype",
        "tool",
    ]
    assert "mailto:labs@example.org" in request.headers.get("User-Agent", "")
    assert request.metadata.get("request_id")
