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

    def fetch_one(
        self, uniprot_id: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return super().fetch_one(uniprot_id, params=params)

    def fetch_batch(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return super().fetch_batch(query, params=params)

    def fetch(
        self, uniprot_id: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        warnings.warn(
            "fetch устарел; используйте fetch_one",  # pragma: no cover - warnings path
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch_one(uniprot_id, params=params)

    def search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        warnings.warn(
            "search устарел; используйте fetch_batch",  # pragma: no cover - warnings path
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch_batch(query, params=params)


__all__ = ["UniProtClient"]
