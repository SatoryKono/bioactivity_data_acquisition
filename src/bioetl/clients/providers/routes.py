from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    ClassVar,
    Iterable,
    Mapping as TypingMapping,
    Protocol,
    runtime_checkable,
)
import warnings

import structlog

from bioetl.clients.base import (
    DataProviderProtocol,
    Page,
    PageStream,
    PaginationParams,
    RecordStream,
    RequestContext,
)
from bioetl.core.http import ApiClientMixin, ClosableMixin
from bioetl.core.http.interfaces import BaseApiClient
from bioetl.core.http.types import JSONRecordStream


@dataclass
class EnricherClientOptions:
    """Опции, передаваемые обогащающим HTTP-клиентам.

    Attributes:
        timeout_sec: Перекрыть таймаут транспортного слоя
            для конкретных вызовов.
        max_retries: Перекрыть количество повторов при ошибках.
        page_key: Ключ в JSON-ответе, содержащий результаты.
        next_key: Ключ перехода на следующую страницу.
        page_param: Имя параметра номерной пагинации (``None`` отключает).
    """

    timeout_sec: float | None = None
    max_retries: int | None = None
    page_key: str | None = "results"
    next_key: str = "next"
    page_param: str | None = "page"


class OptionsAwareApiClient:
    def __init__(
        self,
        api_client: BaseApiClient,
        options: EnricherClientOptions,
    ) -> None:
        self._api_client: BaseApiClient = api_client
        self.timeout_sec: float | None = options.timeout_sec
        self.max_retries: int | None = options.max_retries

    def get_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any] | list[Mapping[str, Any]]:
        del headers
        return self._api_client.get_json(endpoint, params=params)

    def paginate_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[Mapping[str, Any]]:
        result = self._api_client.paginate_json(
            endpoint,
            params=params,
            headers=headers,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )
        return iter(result)

    def iterate_records(
        self,
        *,
        ids: Sequence[str] | None = None,
        page_size: int | None = None,
        fetcher: Callable[[Sequence[str] | None], Any] | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        return self._api_client.iterate_records(
            ids=ids,
            page_size=page_size,
            fetcher=fetcher,
        )

    def close(self) -> None:
        self._api_client.close()

    def fetch_one(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Mapping[str, Any] | list[Mapping[str, Any]]:
        return self._api_client.fetch_one(
            endpoint,
            params=params,
            headers=headers,
            timeout_sec=timeout_sec or self.timeout_sec,
            max_retries=max_retries or self.max_retries,
        )

    def fetch_batch(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
        timeout_sec: float | None = None,
        max_retries: int | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        return self._api_client.fetch_batch(
            endpoint,
            params=params,
            headers=headers,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
            timeout_sec=timeout_sec or self.timeout_sec,
            max_retries=max_retries or self.max_retries,
        )


@runtime_checkable
class EnricherClientProtocol(Protocol):
    def fetch_one(
        self,
        value: str,
        params: Mapping[str, Any] | None = None,
        *,
        route_name: str | None = None,
        page_key: str | None = None,
    ) -> JSONRecordStream:
        ...

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
        ...

    def fetch(
        self, value: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        ...

    def search(
        self, value: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        ...

    def call_route(
        self,
        route_name: str,
        *,
        value: str,
        params: Mapping[str, Any] | None = None,
    ) -> JSONRecordStream:
        ...


class BaseEnricherClient(ClosableMixin, ApiClientMixin):
    """Base client for data enrichment with HTTP API support."""

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
        self._logger = structlog.get_logger(__name__).bind(source=source)

    def _iterate_pages(
        self,
        *,
        path: str,
        params: Mapping[str, Any] | None,
        page_key: str | None,
        next_key: str | None,
        page_param: str | None,
        fetch_pages: Callable[
            [str | None, str, str | None], Iterable[Any]
        ],
        fallback_payload: Any | None = None,
    ) -> JSONRecordStream:
        effective_page_key = (
            page_key if page_key is not None else self.page_key
        )
        effective_next_key = (
            next_key if next_key is not None else self.next_key
        )
        effective_page_param = (
            page_param if page_param is not None else self.page_param
        )

        def iterator() -> Iterator[dict[str, Any]]:
            _ = params
            self._logger.info("api_call", path=path)
            yielded = False
            for payload in fetch_pages(
                effective_page_key, effective_next_key, effective_page_param
            ):
                page_yielded = False
                for item in self._normalize_payload(
                    payload, page_key=effective_page_key
                ):
                    yielded = True
                    page_yielded = True
                    yield item

                if not page_yielded and fallback_payload is not None:
                    yielded = True
                    yield {"result": fallback_payload}

            if not yielded and fallback_payload is not None:
                yield {"result": fallback_payload}

        try:
            yield from self._wrap_iterator(
                iterator, log_context={"path": path}
            )
        except Exception:
            self.close()
            raise

    def fetch_one(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_key: str | None = None,
    ) -> JSONRecordStream:
        try:
            payload = self._wrap_callable(
                lambda: self.api_client.fetch_one(
                    path,
                    params=params,
                    timeout_sec=self.timeout_sec,
                    max_retries=self.max_retries,
                ),
                log_context={"path": path},
            )

            return self._iterate_pages(
                path=path,
                params=params,
                page_key=page_key,
                next_key=None,
                page_param=None,
                fetch_pages=lambda *_: [payload],
                fallback_payload=payload if payload is not None else None,
            )
        except Exception:
            self.close()
            raise

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
        _ = (transport, retries)
        if pagination:
            self._pagination = pagination
        return self

    def _resolve_pagination(
        self, pagination: PaginationParams | None
    ) -> PaginationParams:
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
        # pylint: disable=arguments-differ
        name = route_name or self.DEFAULT_FETCH_ROUTE
        path, params_with_value = self._resolve_route(
            name, value=value, params=params
        )
        return super().fetch_one(
            path, params=params_with_value, page_key=page_key
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
        # pylint: disable=arguments-differ
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

    DEPRECATED_ALIASES: ClassVar[
        TypingMapping[str, str]
    ] = {}

    def __getattr__(self, name: str) -> Any:  # pragma: no cover
        """Proxy deprecated method aliases to their targets."""
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
    """Создаёт класс провайдера на основе конфигурации маршрутов.

    Args:
        name: Имя генерируемого класса.
        source: Имя источника данных.
        routes: Описание маршрутов, по которым строятся запросы.
        deprecated_aliases: Карта устаревших алиасов методов.
        module: Имя модуля, в котором будет объявлен класс (для корректных
            предупреждений и сериализации).
    """

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
