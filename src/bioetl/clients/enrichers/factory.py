from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Callable, Protocol

from bioetl.clients.enrichers.crossref import CrossrefClient
from bioetl.clients.enrichers.openalex import OpenAlexClient
from bioetl.clients.enrichers.pubchem import PubChemClient
from bioetl.clients.enrichers.pubmed import PubmedClient
from bioetl.clients.enrichers.semantic_scholar import SemanticScholarClient
from bioetl.clients.enrichers.uniprot import UniProtClient
from bioetl.clients.enrichers.base import BaseEnricherClient
from bioetl.core.http.interfaces import BaseApiClient


@dataclass(frozen=True)
class EnricherClientOptions:
    """Lightweight options for enricher HTTP clients."""

    timeout_sec: float | None = None
    max_retries: int | None = None


class EnricherApiFactory(Protocol):
    """Callable capable of producing a configured ``BaseApiClient``."""

    def __call__(self, options: EnricherClientOptions) -> BaseApiClient:
        ...


class EnricherEntity(str, Enum):
    """Enumeration of supported enricher providers."""

    CROSSREF = "crossref"
    OPENALEX = "openalex"
    PUBCHEM = "pubchem"
    PUBMED = "pubmed"
    SEMANTIC_SCHOLAR = "semantic_scholar"
    UNIPROT = "uniprot"


class _ConfiguredApiClient:
    def __init__(self, api_client: BaseApiClient, options: EnricherClientOptions):
        self._api_client = api_client
        self.timeout_sec = options.timeout_sec
        self.max_retries = options.max_retries

    def get_json(self, *args, **kwargs):  # pragma: no cover - passthrough
        return self._api_client.get_json(*args, **kwargs)

    def paginate_json(self, *args, **kwargs):  # pragma: no cover - passthrough
        return self._api_client.paginate_json(*args, **kwargs)

    def iterate_records(self, *args, **kwargs):  # pragma: no cover - passthrough
        return self._api_client.iterate_records(*args, **kwargs)

    def close(self) -> None:  # pragma: no cover - passthrough
        close_fn = getattr(self._api_client, "close", None)
        if callable(close_fn):
            close_fn()


class EnricherClientFactory:
    """Factory for creating enricher clients with shared HTTP settings.

    Note:
        API находится в статусе экспериментального: интерфейс может меняться
        по мере эволюции клиентов обогащения.
    """

    def __init__(
        self,
        api_client: BaseApiClient | EnricherApiFactory,
        *,
        options: EnricherClientOptions | None = None,
    ) -> None:
        self._api_client_factory: EnricherApiFactory
        if callable(api_client):
            self._api_client_factory = api_client  # type: ignore[assignment]
        else:
            self._api_client_factory = lambda *_: api_client
        self._options = options or EnricherClientOptions()

    def with_options(self, **overrides) -> "EnricherClientFactory":
        """Return a clone with updated default options."""

        merged = replace(self._options, **overrides)
        return EnricherClientFactory(self._api_client_factory, options=merged)

    def _api(self, **overrides) -> BaseApiClient:
        merged_options = replace(self._options, **overrides) if overrides else self._options
        client = self._api_client_factory(merged_options)
        return _ConfiguredApiClient(client, merged_options)

    def create(self, source: EnricherEntity | str, **overrides) -> BaseEnricherClient:
        entity = EnricherEntity(source)
        factory = getattr(self, entity.value, None)
        if callable(factory):
            return factory(**overrides)
        msg = f"Enricher '{source}' is not supported"
        raise KeyError(msg)

    def crossref(self, **overrides) -> CrossrefClient:
        return CrossrefClient(self._api(**overrides))

    def openalex(self, **overrides) -> OpenAlexClient:
        return OpenAlexClient(self._api(**overrides))

    def pubchem(self, **overrides) -> PubChemClient:
        return PubChemClient(self._api(**overrides))

    def pubmed(self, **overrides) -> PubmedClient:
        return PubmedClient(self._api(**overrides))

    def semantic_scholar(self, **overrides) -> SemanticScholarClient:
        return SemanticScholarClient(self._api(**overrides))

    def uniprot(self, **overrides) -> UniProtClient:
        return UniProtClient(self._api(**overrides))


__all__ = [
    "EnricherClientFactory",
    "EnricherClientOptions",
    "EnricherEntity",
]

