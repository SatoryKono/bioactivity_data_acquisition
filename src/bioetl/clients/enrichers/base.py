from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Iterable, Protocol
from typing import runtime_checkable

import structlog

from bioetl.core.http import ApiClientMixin, ClosableMixin
from bioetl.core.http.interfaces import BaseApiClient
from bioetl.core.http.types import JSONRecordStream


@dataclass
class EnricherClientOptions:
    timeout_sec: float | None = None
    max_retries: int | None = None


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


@runtime_checkable
class EnricherClientProtocol(Protocol):
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
    def __init__(
        self,
        api_client: BaseApiClient,
        source: str,
        *,
        options: EnricherClientOptions | None = None,
    ) -> None:
        effective_options = options or EnricherClientOptions()
        self.api_client = OptionsAwareApiClient(api_client, effective_options)
        self.timeout_sec = effective_options.timeout_sec
        self.max_retries = effective_options.max_retries
        self._logger = structlog.get_logger(__name__).bind(source=source)

    def _get(
        self, path: str, *, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        def iterator() -> Iterator[dict[str, Any]]:
            payload = self._wrap_callable(
                lambda: self.api_client.get_json(path, params=params),
                log_context={"path": path},
            )
            self._logger.info("api_call", path=path)

            yielded = False
            for item in self._normalize_payload(payload):
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


@dataclass(frozen=True)
class RouteConfig:
    name: str
    path: str
    query_param: str | None = None


class RouteEnricherMixin(BaseEnricherClient):
    SOURCE: ClassVar[str]
    ROUTES: ClassVar[Iterable[RouteConfig]]

    def __init__(
        self,
        api_client: BaseApiClient,
        *,
        options: EnricherClientOptions | None = None,
    ) -> None:
        super().__init__(api_client, self.SOURCE, options=options)
        self._route_map = {route.name: route for route in self.ROUTES}

    def _call_route(
        self,
        route_name: str,
        *,
        value: str,
        params: Mapping[str, Any] | None = None,
    ) -> JSONRecordStream:
        route = self._route_map[route_name]
        params_with_value = params
        if route.query_param:
            params_with_value = {route.query_param: value, **(params or {})}

        path = route.path.format(value=value)
        return self._get(path, params=params_with_value)

    def call_route(
        self, route_name: str, *, value: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self._call_route(route_name, value=value, params=params)


__all__ = [
    "BaseEnricherClient",
    "EnricherClientOptions",
    "EnricherClientProtocol",
    "RouteConfig",
    "RouteEnricherMixin",
]
