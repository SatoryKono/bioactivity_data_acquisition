from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any, ClassVar, Iterable

import structlog

from bioetl.core.http import ApiClientMixin, ClosableMixin
from bioetl.core.http.interfaces import BaseApiClient
from bioetl.core.http.types import JSONRecordStream


class BaseEnricherClient(ClosableMixin, ApiClientMixin):
    def __init__(self, api_client: BaseApiClient, source: str) -> None:
        self.api_client = api_client
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

    def __init__(self, api_client: BaseApiClient) -> None:
        super().__init__(api_client, self.SOURCE)
        self._route_map = {route.name: route for route in self.ROUTES}

    def _call_route(
        self, route_name: str, *, value: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        route = self._route_map[route_name]
        params_with_value = params
        if route.query_param:
            params_with_value = {route.query_param: value, **(params or {})}

        path = route.path.format(value=value)
        return self._get(path, params=params_with_value)


__all__ = ["BaseEnricherClient", "RouteConfig", "RouteEnricherMixin"]
