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


class SemanticScholarClient(RouteEnricherMixin):
    SOURCE = "semantic_scholar"
    ROUTES = (
        RouteConfig(name="search", path="/paper/search", query_param="query"),
        RouteConfig(name="fetch", path="/paper/{value}"),
    )

    def fetch_batch(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return super().fetch_batch(query, params=params)

    def fetch_one(
        self, paper_id: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        return super().fetch_one(paper_id, params=params)

    def search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        warnings.warn(
            "search устарел; используйте fetch_batch",  # pragma: no cover - warnings path
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch_batch(query, params=params)

    def fetch(
        self, paper_id: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        warnings.warn(
            "fetch устарел; используйте fetch_one",  # pragma: no cover - warnings path
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch_one(paper_id, params=params)

    def title_search(
        self, query: str, params: Mapping[str, Any] | None = None
    ) -> JSONRecordStream:
        import warnings

        warnings.warn(
            "title_search is deprecated; use search instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch_batch(query, params=params)


__all__ = ["SemanticScholarClient"]
