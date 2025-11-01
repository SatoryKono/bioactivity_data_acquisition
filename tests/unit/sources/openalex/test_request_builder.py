"""Tests for the OpenAlex request builder."""

from bioetl.adapters.base import AdapterConfig
from bioetl.core.api_client import APIConfig
from bioetl.sources.openalex.request import OpenAlexRequestBuilder


def test_builder_appends_mailto_and_sorts_query() -> None:
    """OpenAlex builder should keep deterministic query ordering."""

    api_config = APIConfig(name="openalex", base_url="https://api.openalex.org")
    adapter_config = AdapterConfig(mailto="contact@example.org")
    builder = OpenAlexRequestBuilder(api_config, adapter_config)

    request = builder.build(
        "/works",
        params={"filter": "institutions.id:I123", "search": "aspirin"},
    )

    assert request.url == "https://api.openalex.org/works"
    assert list(request.params.keys()) == ["filter", "mailto", "search"]
    assert request.params["mailto"] == "contact@example.org"
    assert "mailto:contact@example.org" in request.headers.get("User-Agent", "")
    assert request.metadata.get("request_id")
