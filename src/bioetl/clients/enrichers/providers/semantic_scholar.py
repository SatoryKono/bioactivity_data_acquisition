from __future__ import annotations

import warnings

from ..base import DeprecatedAliasMixin, RouteConfig, RouteProviderMixin

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


class SemanticScholarClient(DeprecatedAliasMixin, RouteProviderMixin):
    SOURCE = "semantic_scholar"
    ROUTES = (
        RouteConfig(name="search", path="/paper/search", query_param="query"),
        RouteConfig(name="fetch", path="/paper/{value}"),
    )
    DEPRECATED_ALIASES = {
        "fetch": "fetch_one",
        "search": "fetch_batch",
        "title_search": "fetch_batch",
    }


__all__ = ["SemanticScholarClient"]
