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
from bioetl.clients.enrichers.base import EnricherClientOptions, EnricherClientProtocol
from bioetl.core.http.interfaces import BaseApiClient


@dataclass(frozen=True)
class EnricherEntity(str, Enum):
    """Enumeration of supported enricher client targets."""

    CROSSREF = "crossref"
    OPENALEX = "openalex"
    PUBCHEM = "pubchem"
    PUBMED = "pubmed"
    SEMANTIC_SCHOLAR = "semantic_scholar"
    UNIPROT = "uniprot"


ENRICHER_ALLOWED_ENTITIES: tuple[str, ...] = tuple(member.value for member in EnricherEntity)


class EnricherApiFactory(Protocol):
    """Callable capable of producing a configured ``BaseApiClient``."""

    def __call__(self, options: EnricherClientOptions) -> BaseApiClient:
        ...


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

    def _options_with_overrides(
        self, **overrides: object
    ) -> EnricherClientOptions:
        return replace(self._options, **overrides) if overrides else self._options

    def create(
        self, source: EnricherEntity | str, **overrides: object
    ) -> EnricherClientProtocol:
        entity = EnricherEntity(source)
        factory = getattr(self, entity.value, None)
        if callable(factory):
            return factory(**overrides)
        msg = f"Enricher '{source}' is not supported"
        raise KeyError(msg)

    def crossref(self, **overrides: object) -> CrossrefClient:
        options = self._options_with_overrides(**overrides)
        api_client = self._api_client_factory(options)
        return CrossrefClient(api_client, options=options)

    def openalex(self, **overrides: object) -> OpenAlexClient:
        options = self._options_with_overrides(**overrides)
        api_client = self._api_client_factory(options)
        return OpenAlexClient(api_client, options=options)

    def pubchem(self, **overrides: object) -> PubChemClient:
        options = self._options_with_overrides(**overrides)
        api_client = self._api_client_factory(options)
        return PubChemClient(api_client, options=options)

    def pubmed(self, **overrides: object) -> PubmedClient:
        options = self._options_with_overrides(**overrides)
        api_client = self._api_client_factory(options)
        return PubmedClient(api_client, options=options)

    def semantic_scholar(self, **overrides: object) -> SemanticScholarClient:
        options = self._options_with_overrides(**overrides)
        api_client = self._api_client_factory(options)
        return SemanticScholarClient(api_client, options=options)

    def uniprot(self, **overrides: object) -> UniProtClient:
        options = self._options_with_overrides(**overrides)
        api_client = self._api_client_factory(options)
        return UniProtClient(api_client, options=options)


__all__ = [
    "EnricherEntity",
    "ENRICHER_ALLOWED_ENTITIES",
    "EnricherClientFactory",
    "EnricherClientOptions",
    "EnricherClientProtocol",
]

