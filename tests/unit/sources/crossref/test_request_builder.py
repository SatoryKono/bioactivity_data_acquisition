"""Tests for the Crossref request builder."""

from bioetl.adapters.base import AdapterConfig
from bioetl.core.api_client import APIConfig
from bioetl.sources.crossref.request import CrossrefRequestBuilder


def test_builder_injects_mailto_and_sorts_params() -> None:
    """Builder should apply polite pool headers and deterministic params."""

    api_config = APIConfig(name="crossref", base_url="https://api.crossref.org")
    adapter_config = AdapterConfig(mailto="team@example.org")
    builder = CrossrefRequestBuilder(api_config, adapter_config)

    request = builder.build(
        "/works",
        params={"filter": "type:journal-article", "rows": 20},
    )

    assert request.url == "https://api.crossref.org/works"
    assert list(request.params.keys()) == ["filter", "mailto", "rows"]
    assert request.params["mailto"] == "team@example.org"
    user_agent = request.headers.get("User-Agent", "")
    assert "mailto:team@example.org" in user_agent
    assert request.metadata.get("request_id") is not None
