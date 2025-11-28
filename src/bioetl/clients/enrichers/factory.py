from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Any, Callable, Mapping, Protocol

from bioetl.clients.enrichers.providers import (
    CrossrefClient,
    OpenAlexClient,
    PubChemClient,
    PubmedClient,
    SemanticScholarClient,
    UniProtClient,
)
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

    @classmethod
    def from_config(
        cls, config: Mapping[str, Any] | None
    ) -> "EnricherClientFactory | None":
        """Построить фабрику из конфигурации ``enrichers``.

        Поддерживаются варианты:

        * уже созданная ``EnricherClientFactory`` в ключе ``factory``;
        * ``api_client`` (готовый экземпляр ``BaseApiClient`` или билдер),
          опционально с ``options`` (dict или ``EnricherClientOptions``).

        Если конфигурация присутствует, но валидной фабрики нет, возвращается
        ``NULL_ENRICHER_FACTORY`` для совместимости со стратегиями, которым
        фабрика может не понадобиться.
        """

        if not isinstance(config, Mapping):
            return None

        factory = config.get("factory")
        if isinstance(factory, cls):
            return factory

        api_client = config.get("api_client")
        options_cfg = config.get("options")
        options: EnricherClientOptions | None
        if isinstance(options_cfg, EnricherClientOptions):
            options = options_cfg
        elif isinstance(options_cfg, Mapping):
            options = EnricherClientOptions(**options_cfg)
        else:
            options = None

        if isinstance(api_client, BaseApiClient) or callable(api_client):
            return cls(api_client, options=options)

        return NULL_ENRICHER_FACTORY if config else None

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


class _NullApiClient(BaseApiClient):
    """Заглушка API-клиента для конфигураций без сетевого клиента."""

    def get_json(self, *args, **kwargs):  # pragma: no cover - defensive stub
        raise RuntimeError("Null API client cannot perform requests")

    def paginate_json(self, *args, **kwargs):  # pragma: no cover - defensive stub
        raise RuntimeError("Null API client cannot paginate requests")

    def iterate_records(self, *args, **kwargs):  # pragma: no cover - defensive stub
        raise RuntimeError("Null API client cannot iterate records")

    def close(self) -> None:  # pragma: no cover - defensive stub
        return None


NULL_ENRICHER_FACTORY = EnricherClientFactory(lambda *_: _NullApiClient())


__all__ = [
    "EnricherEntity",
    "ENRICHER_ALLOWED_ENTITIES",
    "EnricherClientFactory",
    "EnricherClientOptions",
    "EnricherClientProtocol",
    "NULL_ENRICHER_FACTORY",
]

