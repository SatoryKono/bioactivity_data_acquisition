from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .base import RouteConfig, RouteEnricherMixin
from bioetl.core.http.types import JSONRecordStream


class SemanticScholarClient(RouteEnricherMixin):
    SOURCE = "semantic_scholar"
    ROUTES = (
        RouteConfig(name="search", path="/paper/search", query_param="query"),
        RouteConfig(name="fetch", path="/paper/{value}"),
    )

    def search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self._call_route("search", value=query, params=params)

    def fetch(
        self, paper_id: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self._call_route("fetch", value=paper_id, params=params)

    title_search = search


__all__ = ["SemanticScholarClient"]
