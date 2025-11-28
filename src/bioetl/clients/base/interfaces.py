"""Unified data provider interfaces and shared dataclasses.

Этот модуль вводит единый контракт для клиентов источников данных,
выровненный по требованиям ETL-пайплайна. Интерфейсы построены вокруг
простых протоколов и dataclass-объектов, чтобы сохранить совместимость с
существующими адаптерами ChEMBL и провайдеров обогащения.
"""
from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, TypeVar, runtime_checkable

from typing_extensions import Self

from .exceptions import (
    ConfigurationError,
    ConnectionError,
    HTTPError,
    PaginationError,
    ProviderError,
    RequestException,
    Timeout,
)

RecordStream = Iterator[dict[str, Any]]


@dataclass(slots=True)
class Page:
    """Единица постраничного результата."""

    items: list[dict[str, Any]]
    next_cursor: str | None = None
    raw: Mapping[str, Any] | None = None


PageStream = Iterator[Page]


@dataclass(slots=True)
class PaginationParams:
    """Параметры пагинации, совместимые с ChEMBL и обогащающими клиентами."""

    page_key: str | None = None
    next_key: str | None = None
    page_param: str | None = None
    page_size: int | None = None

    def override(self, **kwargs: Any) -> "PaginationParams":
        """Создать копию с изменёнными полями."""

        data = {
            "page_key": self.page_key,
            "next_key": self.next_key,
            "page_param": self.page_param,
            "page_size": self.page_size,
        }
        data.update({k: v for k, v in kwargs.items() if v is not None})
        return PaginationParams(**data)


@dataclass(slots=True, frozen=True)
class RequestContext:
    """Дополнительный контекст вызова для логирования и метрик."""

    source: str
    route: str | None = None
    extra: Mapping[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TransportOptions:
    """Унифицированные опции транспорта (таймауты, заголовки)."""

    timeout_sec: float | None = None
    headers: Mapping[str, str] | None = None


@dataclass(slots=True)
class RetryOptions:
    """Параметры ретраев на уровне клиента."""

    max_retries: int | None = None


@runtime_checkable
class SupportsSearch(Protocol):
    """Дополнительный контракт для клиентов с поиском."""

    def search(
        self,
        query: Mapping[str, Any],
        *,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:  # pragma: no cover - протокол
        ...


@runtime_checkable
class SupportsBatch(Protocol):
    """Дополнительный контракт для клиентов с batch-функциями."""

    def fetch_batch(
        self,
        ids: list[str],
        *,
        params: Mapping[str, Any] | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:  # pragma: no cover - протокол
        ...


_T_co = TypeVar("_T_co", covariant=True)


@runtime_checkable
class DataProviderProtocol(Protocol[_T_co]):
    """Базовый контракт клиента источника данных."""

    def fetch_one(
        self,
        ref: str,
        *,
        params: Mapping[str, Any] | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:  # pragma: no cover - протокол
        ...

    def fetch_many(
        self,
        *,
        query: Mapping[str, Any] | None = None,
        page_size: int | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:  # pragma: no cover - протокол
        ...

    def iter_pages(
        self,
        *,
        query: Mapping[str, Any] | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> PageStream:  # pragma: no cover - протокол
        ...

    def configure(
        self,
        *,
        transport: TransportOptions | None = None,
        pagination: PaginationParams | None = None,
        retries: RetryOptions | None = None,
    ) -> Self:  # pragma: no cover - протокол
        ...

    def metadata(self) -> Mapping[str, Any]:  # pragma: no cover - протокол
        ...

    def close(self) -> None:  # pragma: no cover - протокол
        ...


@runtime_checkable
class ClientProtocol(Protocol):
    """Контракт для клиентских адаптеров, поддерживающих ``close``."""

    def close(self) -> None:  # pragma: no cover - протокол
        ...


_FactoryT_co = TypeVar("_FactoryT_co", covariant=True)


class ClientFactory(Protocol[_FactoryT_co]):
    """Фабрика, создающая клиентов для сущностей домена."""

    def create(self, entity: str, mode: str | None = None) -> _FactoryT_co:  # pragma: no cover - протокол
        ...


FACTORIES: dict[str, ClientFactory[Any]] = {}


def register_factory(name: str, factory: ClientFactory[Any]) -> None:
    """Зарегистрировать фабрику по имени домена."""

    FACTORIES[name] = factory


def get_factory(name: str) -> ClientFactory[Any]:
    """Получить фабрику по имени или выбросить ``KeyError``."""

    try:
        return FACTORIES[name]
    except KeyError as exc:  # pragma: no cover - defensive branch
        msg = f"Client factory '{name}' is not registered"
        raise KeyError(msg) from exc


def register_domain_factories(
    *,
    chembl_factory: ClientFactory[Any] | None = None,
    enricher_factory: ClientFactory[Any] | None = None,
) -> dict[str, ClientFactory[Any]]:
    """Утилита регистрации доменных фабрик по умолчанию."""

    if chembl_factory is not None:
        register_factory("chembl", chembl_factory)
    if enricher_factory is not None:
        register_factory("enricher", enricher_factory)
    return FACTORIES


__all__ = [
    "DataProviderProtocol",
    "SupportsBatch",
    "SupportsSearch",
    "PaginationParams",
    "RequestContext",
    "RecordStream",
    "Page",
    "PageStream",
    "TransportOptions",
    "RetryOptions",
    "ProviderError",
    "PaginationError",
    "ConfigurationError",
    "RequestException",
    "HTTPError",
    "Timeout",
    "ConnectionError",
    "ClientProtocol",
    "ClientFactory",
    "FACTORIES",
    "register_factory",
    "register_domain_factories",
    "get_factory",
]
