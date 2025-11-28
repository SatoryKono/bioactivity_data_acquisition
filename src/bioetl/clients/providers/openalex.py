from __future__ import annotations

from ..enricher_base import RouteConfig, create_route_provider_class

OpenAlexClient = create_route_provider_class(
    name="OpenAlexClient",
    source="openalex",
    routes=(
        RouteConfig(name="fetch", path="/works/{value}"),
        RouteConfig(name="search", path="/works", query_param="search"),
    ),
    deprecated_aliases={"fetch": "fetch_one", "search": "fetch_batch"},
    module=__name__,
)


__all__ = ["OpenAlexClient"]
