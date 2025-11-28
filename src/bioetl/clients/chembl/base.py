"""Base ChEMBL client implementations."""
from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Callable, Protocol

from bioetl.clients.chembl.adapter import ChemblTransportAdapter
from bioetl.clients.chembl.pagination import PaginationFactory
from bioetl.core.http.api_entity_client import BaseApiEntityClient
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy
from bioetl.core.http.pagination_helpers import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
)


class BaseChemblEntityProtocol(Protocol):
    """Protocol describing the ChEMBL entity client contract."""

    entity: str

    def __init__(
        self,
        transport: ApiTransportProtocol,
        entity: str,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_factories: Mapping[str, PaginationFactory] | None = None,
    ) -> None:
        """Initialize the ChEMBL entity client."""

    def get(
        self, entity_id: str, *, params: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        """Retrieve an entity by ID."""

    def list(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[dict[str, Any]]:
        """Iterate over paginated entities."""

    def fetch_all(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[dict[str, Any]]:
        """Fetch all entities in the collection."""

    def fetch_by_ids(self, ids: Sequence[str]) -> Iterator[dict[str, Any]]:
        """Fetch entities by their identifiers."""

    def iterate_records(
        self,
        *,
        ids: Sequence[str] | None = None,
        page_size: int | None = None,
        fetcher: Callable[[Sequence[str] | None], Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Iterate over entity records with optional batching."""

    def search(self, params: Mapping[str, Any]) -> Iterator[dict[str, Any]]:
        """Search for entities using query parameters."""

    def status(self) -> Mapping[str, Any]:
        """Return ChEMBL API status information."""

    @property
    def metadata(self) -> Mapping[str, Any]:
        """Expose transport metadata."""


class BaseChemblClient(ChemblTransportAdapter):
    """Compatible ChEMBL transport client over arbitrary transport."""


class ChemblEntityClient(BaseApiEntityClient, BaseChemblEntityProtocol):
    """Client for specific ChEMBL entity types (activity, assay, etc)."""

    def __init__(
        self,
        transport: ApiTransportProtocol,
        entity: str,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_factories: Mapping[str, PaginationFactory] | None = None,
    ) -> None:
        adapter = ChemblTransportAdapter(
            transport,
            pagination_strategy=pagination_strategy,
            pagination_strategy_name=pagination_strategy_name,
            pagination_factories=pagination_factories,
        )
        super().__init__(adapter, adapter.pagination_strategy, entity=entity)

    @property
    def metadata(self) -> Mapping[str, Any]:
        """Return metadata from the underlying transport."""
        base_transport = getattr(self, "transport", None)
        if base_transport is None:
            return {}
        metadata = getattr(base_transport, "metadata", None)
        if isinstance(metadata, Mapping):
            return dict(metadata)
        return {}

    def status(self) -> Mapping[str, Any]:
        """Check ChEMBL API status."""
        return self._wrap_callable(
            lambda: self._transport().request("GET", "/status"),
            log_context={"path": "/status", "method": "GET"},
        )  # type: ignore[return-value]


__all__ = [
    "BaseChemblClient",
    "BaseChemblEntityProtocol",
    "ChemblEntityClient",
    "DEFAULT_PAGE_KEY",
    "DEFAULT_NEXT_KEY",
    "DEFAULT_PAGE_PARAM",
]
