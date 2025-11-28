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


class PubChemClient(DeprecatedAliasMixin, RouteProviderMixin):
    SOURCE = "pubchem"
    ROUTES = (
        RouteConfig(name="fetch", path="/compound/{value}"),
        RouteConfig(name="search", path="/compound/search", query_param="smiles"),
    )
    DEPRECATED_ALIASES = {
        "fetch": "fetch_one",
        "search": "fetch_batch",
        "fetch_by_cid": "fetch_one",
        "search_by_smiles": "fetch_batch",
    }


__all__ = ["PubChemClient"]
