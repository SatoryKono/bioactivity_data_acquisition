from __future__ import annotations

from .routes import RouteConfig, create_route_provider_class

CrossrefClient = create_route_provider_class(
    name="CrossrefClient",
    source="crossref",
    routes=(
        RouteConfig(name="fetch", path="/works/{value}"),
        RouteConfig(name="search", path="/works", query_param="query"),
    ),
    deprecated_aliases={"fetch": "fetch_one", "search": "fetch_batch"},
    module=__name__,
)


__all__ = ["CrossrefClient"]
