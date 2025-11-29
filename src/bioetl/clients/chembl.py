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
class ChemblResourceConfig:
    """
    Конфиг ресурса ChEMBL.

    Здесь задаются только технические детали:
    - относительный endpoint,
    - имя поля идентификатора,
    - ключи фильтров, если нужно маппить абстрактные фильтры → query-параметры.
    """

    endpoint: str                  # "/target", "/activity" и т.п.
    id_field: str                  # "target_chembl_id", "molecule_chembl_id"
    filter_mapping: dict[str, str] | None = None
    pagination: PaginationParams | None = None


class ChemblClient(DataClient):
    """
    Тонкий клиент ChEMBL для одного ресурса.

    Внутри только:
      - имя клиента,
      - имя источника,
      - ссылка на HttpTransport,
      - конфиг ресурса.
    """

    def __init__(
        self,
        *,
        name: str,
        transport: HttpTransport,
    ) -> None:
        self.name = name            # например, "chembl.target"
        source, resource = split_name(name)
        self.source = source
        resource_settings = load_resource_settings(source, resource)
        self._transport = transport
        self._cfg = ChemblResourceConfig(
            endpoint=resource_settings["endpoint"],
            id_field=resource_settings["id_field"],
            filter_mapping=resource_settings.get("filter_mapping"),
            pagination=build_pagination(resource_settings.get("pagination")),
        )

    def fetch_one(
        self,
        request: ClientRequest,
        *,
        context: RequestContext | None = None,
    ) -> Record | None:
        mapped_request = self._map_request(request, single=True)
        return self._transport.fetch_one(
            endpoint=self._cfg.endpoint,
            request=mapped_request,
            context=context,
        )

    def iter_records(
        self,
        request: ClientRequest,
        *,
        context: RequestContext | None = None,
    ) -> RecordStream:
        mapped_request = self._map_request(request, single=False)
        return self._transport.iter_records(
            endpoint=self._cfg.endpoint,
            request=mapped_request,
            context=context,
        )

    def iter_pages(
        self,
        request: ClientRequest,
        *,
        context: RequestContext | None = None,
    ) -> PageStream:
        mapped_request = self._map_request(request, single=False)
        return self._transport.iter_pages(
            endpoint=self._cfg.endpoint,
            request=mapped_request,
            context=context,
        )

    def close(self) -> None:
        close = getattr(self._transport, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> "ChemblClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _map_request(self, request: ClientRequest, *, single: bool) -> ClientRequest:
        """
        Здесь допускается ТОЛЬКО:
          - отображение договорных имён фильтров в реальные query-параметры,
          - подстановка id_field для запросов по id.

        Ни нормализации значений, ни фильтрации по бизнес-правилам.
        """

        if self._cfg.filter_mapping and request.filters:
            mapped_filters: dict[str, Any] = {}
            for k, v in request.filters.items():
                api_key = self._cfg.filter_mapping.get(k, k)
                mapped_filters[api_key] = v
        else:
            mapped_filters = dict(request.filters or {})

        ids = list(request.ids or [])
        if ids:
            mapped_filters[self._cfg.id_field] = ids[0] if single else ids

        return ClientRequest(
            ids=request.ids,
            filters=mapped_filters,
            pagination=request.pagination or self._cfg.pagination,
            raw=request.raw,
        )
