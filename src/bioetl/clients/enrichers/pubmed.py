from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .base import RouteConfig, RouteEnricherMixin
from bioetl.core.http.types import JSONRecordStream


class PubmedClient(RouteEnricherMixin):
    SOURCE = "pubmed"
    ROUTES = (
        RouteConfig(name="search", path="/pubmed", query_param="title"),
        RouteConfig(name="fetch", path="/pubmed/{value}"),
    )

    def search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self.call_route("search", value=query, params=params)

    def fetch(
        self, pmid: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self.call_route("fetch", value=pmid, params=params)

    def search_by_title(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        import warnings

        warnings.warn(
            "search_by_title is deprecated; use search instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.search(query, params=params)

    def fetch_by_pmid(
        self, pmid: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        import warnings

        warnings.warn(
            "fetch_by_pmid is deprecated; use fetch instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch(pmid, params=params)


__all__ = ["PubmedClient"]
