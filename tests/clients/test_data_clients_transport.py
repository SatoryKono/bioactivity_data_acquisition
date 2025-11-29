"""Tests for HTTP data clients delegating work to transport backends."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator

import pytest

from bioetl.clients.base import (
    BaseClient,
    ClientRequest,
    PaginationParams,
    RequestContext,
)
from bioetl.clients.base.http_backend import HttpBackend
from bioetl.clients.base.paging import Page
from bioetl.clients.base.types import Record
from bioetl.clients.config.loader import load_source_config
from bioetl.clients.config.models import (
    ResourceConfig,
    SourceConfig,
)
from bioetl.clients.crossref import CrossrefClient
from bioetl.clients.openalex import OpenAlexClient
from bioetl.clients.pubchem import PubChemClient
from bioetl.clients.pubmed import PubMedClient
from bioetl.clients.semantic_scholar import SemanticScholarClient
from bioetl.clients.uniprot import UniProtClient
from bioetl.clients.chembl import ChemblClient


class FakeBackend(HttpBackend):
    """Test HTTP backend that records calls instead of performing IO."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def fetch_one(
        self,
        *,
        source: SourceConfig,
        resource: ResourceConfig,
        request: ClientRequest,
        context: RequestContext | None = None,
    ) -> Record | None:
        self.calls.append(
            {
                "method": "fetch_one",
                "source": source.source,
                "route": request.route,
                "resource": resource.path,
            }
        )
        return {"ok": True}

    def iter_records(
        self,
        *,
        source: SourceConfig,
        resource: ResourceConfig,
        request: ClientRequest,
        context: RequestContext | None = None,
    ) -> Iterator[Record]:
        self.calls.append(
            {
                "method": "iter_records",
                "source": source.source,
                "route": request.route,
                "resource": resource.path,
            }
        )
        yield {"kind": "record"}

    def iter_pages(
        self,
        *,
        source: SourceConfig,
        resource: ResourceConfig,
        request: ClientRequest,
        context: RequestContext | None = None,
    ) -> Iterator[Page]:
        self.calls.append(
            {
                "method": "iter_pages",
                "source": source.source,
                "route": request.route,
                "resource": resource.path,
            }
        )
        yield Page(items=[{"kind": "page"}])

    def metadata(self, *, source: SourceConfig) -> dict[str, object]:
        return {"source": source.source}

    def close(self) -> None:  # pragma: no cover - trivial
        return None


@dataclass(slots=True)
class ClientCase:
    """Client type, builder name and expected route."""

    client_cls: type[BaseClient]
    builder_name: str
    route: str


CLIENT_CASES = (
    ClientCase(ChemblClient, "request_activity", "activity"),
    ClientCase(PubChemClient, "request_compounds", "compound"),
    ClientCase(PubMedClient, "request_articles", "article"),
    ClientCase(OpenAlexClient, "request_works", "works"),
    ClientCase(CrossrefClient, "request_works", "works"),
    ClientCase(SemanticScholarClient, "request_papers", "paper"),
    ClientCase(UniProtClient, "request_proteins", "protein"),
)


@pytest.mark.parametrize("case", CLIENT_CASES)
def test_clients_delegate_to_backend(case: ClientCase) -> None:
    """Ensure clients delegate requests to the HTTP backend correctly."""

    backend = FakeBackend()
    client = case.client_cls(backend)

    builder = getattr(client, case.builder_name)
    request = builder(
        ids=["x"],
        filters={"k": "v"},
        pagination=PaginationParams(page_size=10),
        context=RequestContext(trace_id="t"),
    )

    assert isinstance(request, ClientRequest)
    assert request.route == case.route

    # fetch_one
    assert client.fetch_one(request) == {"ok": True}
    list(client.iter_records(request))
    list(client.iter_pages(request))

    assert backend.calls[0]["method"] == "fetch_one"
    assert {call["route"] for call in backend.calls} == {case.route}
    assert {
        call["source"] for call in backend.calls
    } == {load_source_config(case.client_cls.source).source}
