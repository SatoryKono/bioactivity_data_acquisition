from __future__ import annotations

from collections.abc import Mapping
from typing import Any
import warnings

from ..base import RouteConfig, RouteEnricherMixin
from bioetl.core.http.types import JSONRecordStream

if __name__.startswith("bioetl.clients.enrichers.") and ".providers." not in __name__:
    module = __name__.split(".")[-1]
    warnings.warn(
        (
            f"Модуль 'bioetl.clients.enrichers.{module}' перемещён в "
            f"'bioetl.clients.enrichers.providers.{module}'"
        ),
        DeprecationWarning,
        stacklevel=2,
    )


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
