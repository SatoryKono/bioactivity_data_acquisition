from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .base import RouteConfig, RouteEnricherMixin
from bioetl.core.http.types import JSONRecordStream


class OpenAlexClient(RouteEnricherMixin):
    SOURCE = "openalex"
    ROUTES = (
        RouteConfig(name="fetch", path="/works/{value}"),
        RouteConfig(name="search", path="/works", query_param="search"),
    )

    def fetch(
        self, oa_id: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self._call_route("fetch", value=oa_id, params=params)

    def search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self._call_route("search", value=query, params=params)


__all__ = ["OpenAlexClient"]
