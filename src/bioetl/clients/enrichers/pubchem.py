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
        return self.call_route("fetch", value=cid, params=params)

    def search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return self.call_route("search", value=query, params=params)

    def fetch_by_cid(
        self, cid: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        import warnings

        warnings.warn(
            "fetch_by_cid is deprecated; use fetch instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch(cid, params=params)

    def search_by_smiles(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        import warnings

        warnings.warn(
            "search_by_smiles is deprecated; use search instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.search(query, params=params)


__all__ = ["PubChemClient"]
