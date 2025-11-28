from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .base import RouteConfig, RouteEnricherMixin
from bioetl.core.http.types import JSONRecordStream


class CrossrefClient(RouteEnricherMixin):
    SOURCE = "crossref"
    ROUTES = (
        RouteConfig(name="fetch", path="/works/{value}"),
        RouteConfig(name="search", path="/works", query_param="query"),
    )

    def fetch(
        self, doi: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self.call_route("fetch", value=doi, params=params)

    def search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self.call_route("search", value=query, params=params)


__all__ = ["CrossrefClient"]
