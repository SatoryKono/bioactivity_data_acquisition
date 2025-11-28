from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from bioetl.clients.enrichers.base import RouteConfig, RouteEnricherMixin
from bioetl.core.http.types import JSONRecordStream


class UniProtClient(RouteEnricherMixin):
    SOURCE = "uniprot"
    ROUTES = (
        RouteConfig(name="fetch", path="/uniprot/{value}"),
        RouteConfig(name="search", path="/uniprot/search", query_param="query"),
    )

    def fetch(
        self, uniprot_id: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self.call_route("fetch", value=uniprot_id, params=params)

    def search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self.call_route("search", value=query, params=params)


__all__ = ["UniProtClient"]
