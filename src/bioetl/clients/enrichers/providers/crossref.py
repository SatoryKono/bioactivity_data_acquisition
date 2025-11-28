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


class CrossrefClient(DeprecatedAliasMixin, RouteProviderMixin):
    SOURCE = "crossref"
    ROUTES = (
        RouteConfig(name="fetch", path="/works/{value}"),
        RouteConfig(name="search", path="/works", query_param="query"),
    )
    DEPRECATED_ALIASES = {"fetch": "fetch_one", "search": "fetch_batch"}


__all__ = ["CrossrefClient"]
