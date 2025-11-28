from __future__ import annotations

from ..enricher_base import RouteConfig, create_route_provider_class

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
