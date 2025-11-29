from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Mapping as TypingMapping, Protocol, runtime_checkable
import warnings

from bioetl.clients.base import (
    DataProviderProtocol,
    Page,
    PageStream,
    PaginationParams,
    RecordStream,
    RequestContext,
)
from bioetl.core.http.base_http_client import BaseHttpClient
from bioetl.core.http.interfaces import BaseApiClient
from bioetl.core.http.pagination_helpers import normalize_payload
from bioetl.core.http.types import JSONRecordStream


@dataclass
class EnricherClientOptions:
    """Опции низкого уровня для обогащающих HTTP-клиентов."""

    timeout_sec: float | None = None
    max_retries: int | None = None
    page_key: str | None = "results"
    next_key: str = "next"
    page_param: str | None = "page"


class OptionsAwareApiClient(BaseHttpClient):
    """Простой адаптер для проксирования таймаутов/ретраев в транспорт."""

    def __init__(
        self,
        api_client: BaseApiClient,
        options: EnricherClientOptions,
    ) -> None:
        super().__init__(
            api_client,
            default_timeout_sec=options.timeout_sec,
            default_max_retries=options.max_retries,
            client_name="enricher_http",
        )
        self._api_client: BaseApiClient = api_client

    def close(self) -> None:  # pragma: no cover - passthrough
        close = getattr(self._api_client, "close", None)
        if callable(close):
            close()


@runtime_checkable
class EnricherClientProtocol(DataProviderProtocol[dict[str, Any]], Protocol):
    def fetch_one(
        self,
        ref: str,
        *,
        params: Mapping[str, Any] | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:
        ...

    def fetch_many(
        self,
        *,
        query: Mapping[str, Any] | None = None,
        page_size: int | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:
        ...

    def iter_pages(
        self,
        *,
        query: Mapping[str, Any] | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> PageStream:
        ...

    def configure(
        self,
        *,
        transport: Any | None = None,
        pagination: PaginationParams | None = None,
        retries: Any | None = None,
    ) -> "EnricherClientProtocol":
        ...

    def metadata(self) -> Mapping[str, Any]:
        ...

    def close(self) -> None:
        ...


class BaseEnricherClient:
    """Минимальный HTTP-клиент для обогащающих источников."""

    def __init__(
        self,
        api_client: BaseApiClient,
        source: str,
        *,
        options: EnricherClientOptions | None = None,
    ) -> None:
        effective_options = options or EnricherClientOptions()
        self.api_client: OptionsAwareApiClient = OptionsAwareApiClient(
            api_client, effective_options
        )  # type: ignore[assignment]
        self.timeout_sec = effective_options.timeout_sec
        self.max_retries = effective_options.max_retries
        self.page_key = effective_options.page_key
        self.next_key = effective_options.next_key
        self.page_param = effective_options.page_param
        self.source = source

    def _normalize_payload(
        self, payload: Any, *, page_key: str | None = None
    ) -> Iterator[dict[str, Any]]:
        yield from normalize_payload(payload, page_key=page_key or self.page_key)

    def _iterate_pages(
        self,
        *,
        path: str,
        params: Mapping[str, Any] | None,
        page_key: str | None,
        next_key: str | None,
        page_param: str | None,
        fetch_pages: Callable[[str | None, str, str | None], Iterable[Any]],
    ) -> JSONRecordStream:
        effective_page_key = page_key if page_key is not None else self.page_key
        effective_next_key = next_key if next_key is not None else self.next_key
        effective_page_param = page_param if page_param is not None else self.page_param

        for payload in fetch_pages(
            effective_page_key, effective_next_key, effective_page_param
        ):
            yield from self._normalize_payload(payload, page_key=effective_page_key)

    def fetch_one(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_key: str | None = None,
    ) -> JSONRecordStream:
        payload = self.api_client.fetch_one(
            path,
            params=params,
            timeout_sec=self.timeout_sec,
            max_retries=self.max_retries,
        )
        return self._iterate_pages(
            path=path,
            params=params,
            page_key=page_key,
            next_key=None,
            page_param=None,
            fetch_pages=lambda *_: [payload],
        )

    def fetch_batch(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_key: str | None = None,
        next_key: str | None = None,
        page_param: str | None = None,
    ) -> JSONRecordStream:
        def _fetch_batch_fn(
            effective_page_key: str | None,
            effective_next_key: str,
            effective_page_param: str | None,
        ) -> Any:
            return self.api_client.fetch_batch(
                path,
                params=params,
                page_key=effective_page_key or "results",
                next_key=effective_next_key,
                page_param=effective_page_param,
                timeout_sec=self.timeout_sec,
                max_retries=self.max_retries,
            )

        return self._iterate_pages(
            path=path,
            params=params,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
            fetch_pages=_fetch_batch_fn,
        )

    def metadata(self) -> Mapping[str, Any]:
        meta = getattr(self.api_client, "metadata", None)
        return meta if isinstance(meta, Mapping) else {}

    def close(self) -> None:
        self.api_client.close()


class UnifiedProviderAdapter(DataProviderProtocol[dict[str, Any]]):
    """Обёртка, приводящая ``RouteProviderBase`` к DataProviderProtocol."""

    def __init__(self, provider: "RouteProviderBase") -> None:
        self._provider = provider
        self._pagination = PaginationParams(
            page_key=getattr(provider, "page_key", None),
            next_key=getattr(provider, "next_key", None),
            page_param=getattr(provider, "page_param", None),
        )

    def configure(
        self,
        *,
        transport: Any | None = None,
        pagination: PaginationParams | None = None,
        retries: Any | None = None,
    ) -> "UnifiedProviderAdapter":
        provider_configure = getattr(self._provider, "configure", None)
        if callable(provider_configure):
            provider_configure(
                transport=transport, pagination=pagination, retries=retries
            )
        elif transport or retries:
            _ = (transport, retries)

        if pagination:
            self._pagination = pagination
        return self

    def _resolve_pagination(self, pagination: PaginationParams | None) -> PaginationParams:
        return self._pagination.override(
            page_key=pagination.page_key if pagination else None,
            next_key=pagination.next_key if pagination else None,
            page_param=pagination.page_param if pagination else None,
            page_size=pagination.page_size if pagination else None,
        )

    def _extract_value(self, query: Mapping[str, Any] | None) -> tuple[str, dict[str, Any]]:
        query_map = dict(query or {})
        try:
            value = query_map.pop("value")
        except KeyError as exc:  # pragma: no cover - defensive branch
            from bioetl.clients import exceptions

            raise exceptions.ConfigurationError(
                "query must contain 'value' for route providers"
            ) from exc
        return str(value), query_map

    def fetch_one(
        self,
        ref: str,
        *,
        params: Mapping[str, Any] | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:
        route_name = context.route if context else None
        return self._provider.fetch_one(
            ref, params=params, route_name=route_name, page_key=self._pagination.page_key
        )

    def iter_pages(
        self,
        *,
        query: Mapping[str, Any] | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> PageStream:
        route_name = context.route if context else None
        value, remaining = self._extract_value(query)
        effective = self._resolve_pagination(pagination)

        path, params_with_value = self._provider._resolve_route(  # type: ignore[attr-defined]
            route_name or self._provider.DEFAULT_SEARCH_ROUTE,
            value=value,
            params=remaining,
        )
        params_with_value = params_with_value or {}
        if effective.page_size:
            params_with_value.setdefault("limit", effective.page_size)

        effective_next = effective.next_key or self._provider.next_key or "next"
        pages = self._provider.api_client.paginate_json(
            path,
            params=params_with_value,
            page_key=effective.page_key or self._provider.page_key or "results",
            next_key=effective_next,
            page_param=effective.page_param
            if effective.page_param is not None
            else self._provider.page_param,
        )

        for raw_page in pages:
            items = list(
                self._provider._normalize_payload(  # type: ignore[attr-defined]
                    raw_page, page_key=effective.page_key
                )
            )
            next_cursor = raw_page.get(effective_next) if isinstance(raw_page, Mapping) else None
            yield Page(
                items=items,
                next_cursor=next_cursor,
                raw=raw_page if isinstance(raw_page, Mapping) else None,
            )

    def fetch_many(
        self,
        *,
        query: Mapping[str, Any] | None = None,
        page_size: int | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:
        effective_pagination = pagination
        if page_size is not None:
            base = pagination or self._pagination
            effective_pagination = base.override(page_size=page_size)
        for page in self.iter_pages(
            query=query, pagination=effective_pagination, context=context
        ):
            yield from page.items

    def metadata(self) -> Mapping[str, Any]:
        meta = getattr(self._provider, "metadata", None)
        if callable(meta):
            meta = meta()
        if isinstance(meta, Mapping):
            return meta

        meta = getattr(self._provider.api_client, "metadata", None)
        return meta if isinstance(meta, Mapping) else {}

    def close(self) -> None:
        self._provider.close()


@dataclass(frozen=True)
class RouteConfig:
    name: str
    path: str
    query_param: str | None = None


class RouteEnricherMixin(BaseEnricherClient):
    SOURCE: ClassVar[str]
    ROUTES: ClassVar[Iterable[RouteConfig]]

    DEFAULT_FETCH_ROUTE: ClassVar[str] = "fetch"
    DEFAULT_SEARCH_ROUTE: ClassVar[str] = "search"

    def __init__(
        self,
        api_client: BaseApiClient,
        *,
        options: EnricherClientOptions | None = None,
    ) -> None:
        super().__init__(api_client, self.SOURCE, options=options)
        self._route_map = {route.name: route for route in self.ROUTES}

    def _resolve_route(
        self, route_name: str, *, value: str, params: Mapping[str, Any] | None
    ) -> tuple[str, Mapping[str, Any] | None]:
        route = self._route_map[route_name]
        params_with_value = params
        if route.query_param:
            params_with_value = {
                route.query_param: value,
                **(params or {}),
            }

        path = route.path.format(value=value)
        return path, params_with_value

    def fetch_one(
        self,
        value: str,
        params: Mapping[str, Any] | None = None,
        *,
        route_name: str | None = None,
        page_key: str | None = None,
    ) -> JSONRecordStream:
        name = route_name or self.DEFAULT_FETCH_ROUTE
        path, params_with_value = self._resolve_route(
            name, value=value, params=params
        )
        return super().fetch_one(path, params=params_with_value, page_key=page_key)

    def fetch_batch(
        self,
        value: str,
        params: Mapping[str, Any] | None = None,
        *,
        route_name: str | None = None,
        page_key: str | None = None,
        next_key: str | None = None,
        page_param: str | None = None,
    ) -> JSONRecordStream:
        name = route_name or self.DEFAULT_SEARCH_ROUTE
        path, params_with_value = self._resolve_route(
            name, value=value, params=params
        )
        return super().fetch_batch(
            path,
            params=params_with_value,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )

    def call_route(
        self,
        route_name: str,
        *,
        value: str,
        params: Mapping[str, Any] | None = None,
    ) -> JSONRecordStream:
        warnings.warn(
            "call_route устарел; используйте fetch_one/fetch_batch",
            DeprecationWarning,
            stacklevel=2,
        )
        path, params_with_value = self._resolve_route(
            route_name, value=value, params=params
        )
        return super().fetch_batch(path, params=params_with_value)


class RouteProviderMixin(RouteEnricherMixin):
    """Базовый миксин для провайдеров с маршрутизацией по ROUTES."""

    ROUTES: ClassVar[Iterable[RouteConfig]]
    SOURCE: ClassVar[str]

    def fetch_one(
        self,
        value: str,
        params: Mapping[str, Any] | None = None,
        *,
        route_name: str | None = None,
        page_key: str | None = None,
    ) -> JSONRecordStream:
        return super().fetch_one(
            value, params=params, route_name=route_name, page_key=page_key
        )

    def fetch_batch(
        self,
        value: str,
        params: Mapping[str, Any] | None = None,
        *,
        route_name: str | None = None,
        page_key: str | None = None,
        next_key: str | None = None,
        page_param: str | None = None,
    ) -> JSONRecordStream:
        return super().fetch_batch(
            value,
            params=params,
            route_name=route_name,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )


class DeprecatedAliasMixin:
    """Автоматически проксирует устаревшие алиасы методов."""

    DEPRECATED_ALIASES: ClassVar[TypingMapping[str, str]] = {}

    def __getattr__(self, name: str) -> Any:  # pragma: no cover
        alias_target = self.DEPRECATED_ALIASES.get(name)
        if alias_target:
            target = getattr(self, alias_target)

            def _wrapper(*args: Any, **kwargs: Any) -> Any:
                warnings.warn(
                    f"{name} устарел; используйте {alias_target}",
                    DeprecationWarning,
                    stacklevel=2,
                )
                return target(*args, **kwargs)

            return _wrapper
        raise AttributeError(
            f"{self.__class__.__name__!s} has no attribute {name!r}"
        )


class RouteProviderBase(DeprecatedAliasMixin, RouteProviderMixin):
    """Базовый класс для провайдеров, создаваемых фабрикой."""

    SOURCE: ClassVar[str]
    ROUTES: ClassVar[Iterable[RouteConfig]]
    DEPRECATED_ALIASES: ClassVar[TypingMapping[str, str]] = {}


def create_route_provider_class(
    *,
    name: str,
    source: str,
    routes: Iterable[RouteConfig],
    deprecated_aliases: TypingMapping[str, str] | None = None,
    module: str | None = None,
) -> type[RouteProviderBase]:
    """Создаёт класс провайдера на основе конфигурации маршрутов."""

    class_attributes: dict[str, Any] = {
        "SOURCE": source,
        "ROUTES": tuple(routes),
        "DEPRECATED_ALIASES": dict(deprecated_aliases or {}),
    }

    if module:
        class_attributes["__module__"] = module

    return type(name, (RouteProviderBase,), class_attributes)


__all__ = [
    "BaseEnricherClient",
    "EnricherClientOptions",
    "EnricherClientProtocol",
    "RouteConfig",
    "RouteEnricherMixin",
    "RouteProviderMixin",
    "DeprecatedAliasMixin",
    "RouteProviderBase",
    "UnifiedProviderAdapter",
    "create_route_provider_class",
]
