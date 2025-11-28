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
        return self.call_route("search", value=query, params=params)

    def fetch(
        self, paper_id: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self.call_route("fetch", value=paper_id, params=params)

    def title_search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        import warnings

        warnings.warn(
            "title_search is deprecated; use search instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.search(query, params=params)


__all__ = ["SemanticScholarClient"]
