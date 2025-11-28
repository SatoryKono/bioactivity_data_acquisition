from __future__ import annotations

from ..base import RouteConfig, create_route_provider_class
from .utils import warn_on_provider_module_move

warn_on_provider_module_move(__name__)

UniProtClient = create_route_provider_class(
    name="UniProtClient",
    source="uniprot",
    routes=(
        RouteConfig(name="fetch", path="/uniprot/{value}"),
        RouteConfig(name="search", path="/uniprot/search", query_param="query"),
    ),
    deprecated_aliases={"fetch": "fetch_one", "search": "fetch_batch"},
    module=__name__,
)


__all__ = ["UniProtClient"]
