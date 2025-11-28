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


class PubmedClient(DeprecatedAliasMixin, RouteProviderMixin):
    SOURCE = "pubmed"
    ROUTES = (
        RouteConfig(name="search", path="/pubmed", query_param="title"),
        RouteConfig(name="fetch", path="/pubmed/{value}"),
    )
    DEPRECATED_ALIASES = {
        "fetch": "fetch_one",
        "search": "fetch_batch",
        "search_by_title": "fetch_batch",
        "fetch_by_pmid": "fetch_one",
    }


__all__ = ["PubmedClient"]
