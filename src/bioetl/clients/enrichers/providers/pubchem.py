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


class PubChemClient(RouteEnricherMixin):
    SOURCE = "pubchem"
    ROUTES = (
        RouteConfig(name="fetch", path="/compound/{value}"),
        RouteConfig(name="search", path="/compound/search", query_param="smiles"),
    )

    def fetch_one(
        self, cid: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return super().fetch_one(cid, params=params)

    def fetch_batch(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return super().fetch_batch(query, params=params)

    def fetch(
        self, cid: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        warnings.warn(
            "fetch устарел; используйте fetch_one",  # pragma: no cover - warnings path
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch_one(cid, params=params)

    def search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        warnings.warn(
            "search устарел; используйте fetch_batch",  # pragma: no cover - warnings path
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch_batch(query, params=params)

    def fetch_by_cid(
        self, cid: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        import warnings

        warnings.warn(
            "fetch_by_cid is deprecated; use fetch instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch_one(cid, params=params)

    def search_by_smiles(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        warnings.warn(
            "search_by_smiles is deprecated; use search instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch_batch(query, params=params)


__all__ = ["PubChemClient"]
