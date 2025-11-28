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
        return self._call_route("search", value=query, params=params)

    def fetch(
        self, pmid: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self._call_route("fetch", value=pmid, params=params)

    search_by_title = search
    fetch_by_pmid = fetch


__all__ = ["PubmedClient"]
