from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from bioetl.clients.base.client import (
    ClientRequest,
    DataClient,
    PaginationParams,
    PageStream,
    Record,
    RecordStream,
    RequestContext,
)
from bioetl.clients.utils.config_loader import (
    build_pagination,
    load_resource_settings,
    split_name,
)
from bioetl.core.http.transport import HttpTransport


@dataclass(frozen=True)
class PubchemResourceConfig:
    """
    Конфиг ресурса PUBCHEM.

    Только технические детали:
    - относительный endpoint,
    - имя поля идентификатора,
    - отображение абстрактных фильтров → query-параметры API.
    """

    endpoint: str
    id_field: str
    filter_mapping: dict[str, str] | None = None
    pagination: PaginationParams | None = None


class PubchemCompoundClient(DataClient):
    source = "pubchem"

    def __init__(
        self,
        name: str,
        transport: HttpTransport,
    ) -> None:
        self.name = name
        source, resource = split_name(name)
        self.source = source
        resource_settings = load_resource_settings(source, resource)
        self._resource_config = PubchemResourceConfig(
            endpoint=resource_settings["endpoint"],
            id_field=resource_settings["id_field"],
            filter_mapping=resource_settings.get("filter_mapping"),
            pagination=build_pagination(resource_settings.get("pagination")),
        )
        self._transport = transport

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

    def __enter__(self) -> "PubchemCompoundClient":
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
            "pagination": request.pagination or self._resource_config.pagination,
            "raw": request.raw,
        }


__all__ = ["PubchemCompoundClient", "PubchemResourceConfig"]
