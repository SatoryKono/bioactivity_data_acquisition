"""Base ChEMBL client implementations."""
from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Callable, Protocol

from bioetl.clients.chembl.adapter import ChemblTransportAdapter
from bioetl.clients.chembl.compat import ChemblCompatibilityMixin
from bioetl.clients.chembl.pagination import (
    PaginationFactory,
    create_pagination_strategy,
)
from bioetl.core.http.api_entity_client import BaseApiEntityClient
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy
from bioetl.core.http.pagination_helpers import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
)


class ChemblClientProtocol(Protocol):
    """Protocol describing the ChEMBL client contract."""

    entity: str

    def get(
        self, entity_id: str, *, params: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        """Retrieve an entity by ID."""

    def fetch_one(
        self, entity_id: str, *, params: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        """Retrieve a single entity payload."""

    def fetch_many(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[dict[str, Any]]:
        """Iterate over paginated entities."""

    def fetch_batch(
        self,
        ids: Sequence[str],
        *,
        params: Mapping[str, Any] | None = None,
        path_template: str = "/{entity}/{id}",
    ) -> Iterator[dict[str, Any]]:
        """Fetch entities by their identifiers."""

    def fetch_by_ids(self, ids: Sequence[str]) -> Iterator[dict[str, Any]]:
        """Deprecated alias for ``fetch_batch``."""

    def fetch_page(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[dict[str, Any]]:
        """Deprecated alias for ``fetch_many``."""

    def list(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[dict[str, Any]]:
        """Deprecated alias for ``fetch_many``."""

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

    def fetch_batch(self, ids: Sequence[str]) -> Iterator[dict[str, Any]]:
        """Fetch a batch of entities by their identifiers."""

    def fetch_one(
        self, entity_id: str, *, params: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        """Fetch a single entity by its identifier."""

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


class BaseChemblEntityProtocol(ChemblClientProtocol, Protocol):
    """Legacy alias for configured ChEMBL entity clients."""


class BaseChemblClient(
    ChemblCompatibilityMixin, BaseApiEntityClient, ChemblClientProtocol
):
    """Base ChEMBL client implementing common operations and aliases."""

    def __init__(
        self,
        transport: ApiTransportProtocol,
        entity: str,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_factories: Mapping[str, PaginationFactory] | None = None,
    ) -> None:
        strategy = pagination_strategy or getattr(transport, "pagination_strategy", None)
        if strategy is None:
            strategy = create_pagination_strategy(
                pagination_strategy_name, factories=pagination_factories
            )
        if strategy is None:
            msg = "Pagination strategy is required for BaseChemblClient"
            raise ValueError(msg)
        super().__init__(transport, strategy, entity=entity)

    def fetch_one(
        self, entity_id: str, *, params: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        """Fetch a single ChEMBL entity."""

        return super().fetch_one(entity_id, params=params)

    def fetch_batch(
        self,
        ids: Sequence[str],
        *,
        params: Mapping[str, Any] | None = None,
        path_template: str = "/{entity}/{id}",
    ) -> Iterator[dict[str, Any]]:
        """Fetch multiple entities by IDs using the base implementation."""

        return super().fetch_batch(
            ids, params=params, path_template=path_template
        )

    def fetch_many(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[dict[str, Any]]:
        """Iterate over paginated entities via the base client."""

        return super().fetch_many(
            page_size=page_size,
            params=params,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )

    def fetch_one(
        self, entity_id: str, *, params: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        """Fetch a single entity by ID."""

        return super().fetch_one(entity_id, params=params)

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


class ChemblEntityClient(BaseChemblClient):
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
        super().__init__(
            adapter,
            entity,
            pagination_strategy=adapter.pagination_strategy,
        )


__all__ = [
    "BaseChemblClient",
    "BaseChemblEntityProtocol",
    "ChemblClientProtocol",
    "ChemblEntityClient",
    "DEFAULT_PAGE_KEY",
    "DEFAULT_NEXT_KEY",
    "DEFAULT_PAGE_PARAM",
]
