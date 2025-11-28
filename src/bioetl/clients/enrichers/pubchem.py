from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .base import RouteConfig, RouteEnricherMixin
from bioetl.core.http.types import JSONRecordStream


class PubChemClient(RouteEnricherMixin):
    SOURCE = "pubchem"
    ROUTES = (
        RouteConfig(name="fetch", path="/compound/{value}"),
        RouteConfig(name="search", path="/compound/search", query_param="smiles"),
    )

    def fetch(
        self, cid: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self._call_route("fetch", value=cid, params=params)

    def search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self._call_route("search", value=query, params=params)

    fetch_by_cid = fetch
    search_by_smiles = search


__all__ = ["PubChemClient"]
