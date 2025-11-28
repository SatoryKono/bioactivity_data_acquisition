from __future__ import annotations

from ..base import RouteConfig, create_route_provider_class
from .utils import warn_on_provider_module_move

warn_on_provider_module_move(__name__)

PubmedClient = create_route_provider_class(
    name="PubmedClient",
    source="pubmed",
    routes=(
        RouteConfig(name="search", path="/pubmed", query_param="title"),
        RouteConfig(name="fetch", path="/pubmed/{value}"),
    ),
    deprecated_aliases={
        "fetch": "fetch_one",
        "search": "fetch_batch",
        "search_by_title": "fetch_batch",
        "fetch_by_pmid": "fetch_one",
    },
    module=__name__,
)


__all__ = ["PubmedClient"]
