"""Factories producing protocol-compliant client adapters."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from bioetl.clients.chembl_entity_factory import ChemblClientBundle, ChemblEntityClientFactory

from .chembl_adapter import ChemblAdapter
from .protocols import ChemblClientFactoryProtocol


class _ChemblAdapterFactory(ChemblClientFactoryProtocol):
    """Wrap :class:`ChemblEntityClientFactory` with protocol-compliant adapters."""

    def __init__(self, config: Any, *, api_client_factory: Any | None = None) -> None:
        self._factory = ChemblEntityClientFactory(
            config, api_client_factory=api_client_factory
        )

    def build(
        self,
        entity_name: str,
        *,
        source_name: str = "chembl",
        source_config: Any | None = None,
        options: Any | None = None,
        chembl_client_kwargs: Any | None = None,
        fresh_http_client: bool = False,
    ) -> ChemblClientBundle:
        bundle = self._factory.build(
            entity_name,
            source_name=source_name,
            source_config=source_config,
            options=options,
            chembl_client_kwargs=chembl_client_kwargs,
            fresh_http_client=fresh_http_client,
        )
        adapted_client = (
            ChemblAdapter(bundle.entity_client)
            if bundle.entity_client is not None
            else None
        )
        return replace(bundle, entity_client=adapted_client)

    def build_http_client(
        self,
        *,
        source_name: str = "chembl",
        source_config: Any | None = None,
        options: Any | None = None,
        fresh_http_client: bool = False,
    ) -> tuple[Any, str, Any]:
        """Delegate HTTP client construction to the underlying factory."""

        return self._factory.build_http_client(
            source_name=source_name,
            source_config=source_config,
            options=options,
            fresh_http_client=fresh_http_client,
        )


def default_chembl_factory(
    config: Any, *, api_client_factory: Any | None = None
) -> ChemblClientFactoryProtocol:
    """Return default factory producing Chembl protocol adapters from config."""

    return _ChemblAdapterFactory(config, api_client_factory=api_client_factory)
