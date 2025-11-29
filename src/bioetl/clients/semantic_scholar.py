from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from bioetl.clients.base.client import (
    ClientRequest,
    DataClient,
    PageStream,
    Record,
    RecordStream,
    RequestContext,
)
from bioetl.core.http.transport import HttpTransport


@dataclass(frozen=True)
class SemanticScholarResourceConfig:
    """
    Конфиг ресурса SEMANTIC_SCHOLAR.

    Только технические детали:
    - относительный endpoint,
    - имя поля идентификатора,
    - отображение абстрактных фильтров → query-параметры API.
    """

    endpoint: str
    id_field: str
    filter_mapping: dict[str, str] | None = None


class SemanticScholarPaperClient(DataClient):
    source = "semantic_scholar"

    def __init__(
        self,
        name: str,
        transport: HttpTransport,
        resource_config: SemanticScholarResourceConfig,
    ) -> None:
        self.name = name
        self._transport = transport
        self._resource_config = resource_config

    def fetch_one(
        self, request: ClientRequest, *, context: RequestContext | None = None
    ) -> Record | None:
        mapped_request = self._map_request(request)
        return self._transport.fetch_one(
            endpoint=mapped_request["endpoint"],
            params=mapped_request["params"],
            pagination=mapped_request["pagination"],
            raw=mapped_request["raw"],
            context=context,
        )

    def iter_records(
        self, request: ClientRequest, *, context: RequestContext | None = None
    ) -> RecordStream:
        mapped_request = self._map_request(request)
        return self._transport.iter_records(
            endpoint=mapped_request["endpoint"],
            params=mapped_request["params"],
            pagination=mapped_request["pagination"],
            raw=mapped_request["raw"],
            context=context,
        )

    def iter_pages(
        self, request: ClientRequest, *, context: RequestContext | None = None
    ) -> PageStream:
        mapped_request = self._map_request(request)
        return self._transport.iter_pages(
            endpoint=mapped_request["endpoint"],
            params=mapped_request["params"],
            pagination=mapped_request["pagination"],
            raw=mapped_request["raw"],
            context=context,
        )

    def close(self) -> None:
        self._transport.close()

    def __enter__(self) -> "SemanticScholarPaperClient":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> None:
        self.close()

    def _map_request(self, request: ClientRequest) -> dict[str, Any]:
        filter_mapping = self._resource_config.filter_mapping or {}
        params: dict[str, Any] = {}

        if request.filters:
            params.update(
                {
                    filter_mapping.get(key, key): value
                    for key, value in request.filters.items()
                }
            )

        if request.ids:
            params[self._resource_config.id_field] = request.ids

        return {
            "endpoint": self._resource_config.endpoint,
            "params": params or None,
            "pagination": request.pagination,
            "raw": request.raw,
        }


__all__ = ["SemanticScholarPaperClient", "SemanticScholarResourceConfig"]
