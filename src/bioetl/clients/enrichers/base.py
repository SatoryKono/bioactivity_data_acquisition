from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any, ClassVar, Iterable, Mapping as TypingMapping, Protocol, runtime_checkable
import warnings

import structlog

from bioetl.core.http import ApiClientMixin, ClosableMixin
from bioetl.core.http.interfaces import BaseApiClient
from bioetl.core.http.types import JSONRecordStream


@dataclass
class EnricherClientOptions:
    """Опции, передаваемые обогащающим HTTP-клиентам.

    Attributes:
        timeout_sec: Перекрыть таймаут транспортного слоя для конкретных вызовов.
        max_retries: Перекрыть количество повторов при ошибках.
        page_key: Ключ в JSON-ответе, содержащий результаты.
        next_key: Ключ перехода на следующую страницу.
        page_param: Имя параметра номерной пагинации (``None`` отключает его).
    """

    timeout_sec: float | None = None
    max_retries: int | None = None
    page_key: str | None = "results"
    next_key: str = "next"
    page_param: str | None = "page"


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
        self, route_name: str, *, value: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        ...


class BaseEnricherClient(ClosableMixin, ApiClientMixin):
    def __init__(
        self,
        api_client: BaseApiClient,
        source: str,
        *,
        options: EnricherClientOptions | None = None,
    ) -> None:
        self.api_client = api_client
        effective_options = options or EnricherClientOptions()
        self.timeout_sec = effective_options.timeout_sec
        self.max_retries = effective_options.max_retries
        self.page_key = effective_options.page_key
        self.next_key = effective_options.next_key
        self.page_param = effective_options.page_param
        self._logger = structlog.get_logger(__name__).bind(source=source)

    def fetch_one(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        page_key: str | None = None,
    ) -> JSONRecordStream:
        def iterator() -> Iterator[dict[str, Any]]:
            payload = self._wrap_callable(
                lambda: self.api_client.fetch_one(
                    path,
                    params=params,
                    timeout_sec=self.timeout_sec,
                    max_retries=self.max_retries,
                ),
                log_context={"path": path},
            )
            self._logger.info("api_call", path=path)

            effective_page_key = page_key if page_key is not None else self.page_key
            yielded = False
            for item in self._normalize_payload(payload, page_key=effective_page_key):
                yielded = True
                yield item

            if not yielded and payload is not None:
                yield {"result": payload}

        try:
            yield from self._wrap_iterator(
                iterator, log_context={"path": path}
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
        effective_page_key = page_key if page_key is not None else self.page_key
        effective_next_key = next_key if next_key is not None else self.next_key
        effective_page_param = (
            page_param if page_param is not None else self.page_param
        )

        def iterator() -> Iterator[dict[str, Any]]:
            self._logger.info("api_call", path=path)
            yielded = False
            for payload in self.api_client.fetch_batch(
                path,
                params=params,
                page_key=effective_page_key or "results",
                next_key=effective_next_key,
                page_param=effective_page_param,
                timeout_sec=self.timeout_sec,
                max_retries=self.max_retries,
            ):
                yielded = True
                yield from self._normalize_payload(
                    payload, page_key=effective_page_key
                )

            if not yielded:
                return

        try:
            yield from self._wrap_iterator(
                iterator, log_context={"path": path}
            )
        except Exception:
            self.close()
            raise


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
            params_with_value = {route.query_param: value, **(params or {})}

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
        path, params_with_value = self._resolve_route(name, value=value, params=params)
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
        name = route_name or self.DEFAULT_SEARCH_ROUTE
        path, params_with_value = self._resolve_route(name, value=value, params=params)
        return super().fetch_batch(
            path,
            params=params_with_value,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )

    def call_route(
        self, route_name: str, *, value: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        warnings.warn(
            "call_route устарел; используйте fetch_one/fetch_batch",  # pragma: no cover - warnings path
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

    def __getattr__(self, name: str) -> Any:  # pragma: no cover - wrapper path
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
        raise AttributeError(f"{self.__class__.__name__!s} has no attribute {name!r}")


__all__ = [
    "BaseEnricherClient",
    "EnricherClientOptions",
    "EnricherClientProtocol",
    "RouteConfig",
    "RouteEnricherMixin",
    "RouteProviderMixin",
    "DeprecatedAliasMixin",
]
