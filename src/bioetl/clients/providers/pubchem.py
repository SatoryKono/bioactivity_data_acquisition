from __future__ import annotations

from .routes import RouteConfig, create_route_provider_class

PubChemClient = create_route_provider_class(
    name="PubChemClient",
    source="pubchem",
    routes=(
        RouteConfig(name="fetch", path="/compound/{value}"),
        RouteConfig(
            name="search", path="/compound/search", query_param="smiles"
        ),
    ),
    deprecated_aliases={
        "fetch": "fetch_one",
        "search": "fetch_batch",
        "fetch_by_cid": "fetch_one",
        "search_by_smiles": "fetch_batch",
    },
    module=__name__,
)


__all__ = ["PubChemClient"]
