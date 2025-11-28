from __future__ import annotations

from ..base import RouteConfig, create_route_provider_class
from .utils import warn_on_provider_module_move

warn_on_provider_module_move(__name__)

SemanticScholarClient = create_route_provider_class(
    name="SemanticScholarClient",
    source="semantic_scholar",
    routes=(
        RouteConfig(name="search", path="/paper/search", query_param="query"),
        RouteConfig(name="fetch", path="/paper/{value}"),
    ),
    deprecated_aliases={
        "fetch": "fetch_one",
        "search": "fetch_batch",
        "title_search": "fetch_batch",
    },
    module=__name__,
)


__all__ = ["SemanticScholarClient"]
