"""Base API entity client implementation."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, Protocol, cast

from bioetl.core.http.client_mixins import ApiClientMixin, ClosableMixin
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
    PaginationStrategy,
)
from bioetl.core.http.pagination_helpers import (
    iter_ids,
    iterate_records,
    list_entities,
    warn_fetch_all,
)

import structlog


class EntityClientProtocol(Protocol):
    """Protocol for entity clients."""

    entity: str

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
    ) -> Iterator[Mapping[str, Any]]:
        """Retrieve a list of entities."""

    def fetch_by_ids(
        self, ids: Sequence[str]
    ) -> Iterator[Mapping[str, Any]]:
        """Retrieve entities by IDs."""

    def fetch_one(
        self, entity_id: str, *, params: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        """Fetch a single entity with explicit naming."""

    def fetch_batch(
        self,
        *,
        ids: Sequence[str],
        page_size: int | None = None,
        fetcher: Callable[[Sequence[str]], Any] | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Fetch a batch of entities using the client's batching logic."""

    def search(
        self, params: Mapping[str, Any]
    ) -> Iterator[Mapping[str, Any]]:
        """Search for entities."""

    def close(self) -> None:
        """Close the client."""


class BaseApiEntityClient(ApiClientMixin, ClosableMixin):
    """Базовый клиент сущности API с общей логикой обхода записей."""

    def __init__(
        self,
        transport: ApiTransportProtocol,
        pagination: PaginationStrategy,
        *,
        entity: str,
    ) -> None:
        """Initialize the client.

        Args:
            transport: The API transport protocol.
            pagination: The pagination strategy.
            entity: The entity name.
        """
        self.transport = transport
        self.entity = entity.strip("/")
        self.pagination_strategy = pagination
        self._logger = structlog.get_logger(__name__).bind(entity=self.entity)

    def _entity_path(self, suffix: str | None = None) -> str:
        """Get the entity path.

        Args:
            suffix: The path suffix.

        Returns:
            The entity path.
        """
        if not suffix:
            return f"/{self.entity}"
        suffix = str(suffix).lstrip("/")
        return f"/{self.entity}/{suffix}"

    def iter_ids(
        self,
        ids: Sequence[str],
        path_template: str = "/{entity}/{id}",
    ) -> Iterator[dict[str, Any]]:
        """Iterate over entity IDs.

        Args:
            ids: The entity IDs.
            path_template: The path template.

        Returns:
            An iterator over entity IDs.
        """
        return iter_ids(
            ids=ids,
            entity=self.entity,
            transport=self._transport(),
            normalize=self._normalize_payload,
            wrap_callable=self._wrap_callable,
            wrap_iterator=self._wrap_iterator,
            logger=self._logger,
            path_template=path_template,
        )

    def get(
        self, entity_id: str, *, params: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        """Retrieve an entity by ID.

        Args:
            entity_id: The entity ID.
            params: The request parameters.

        Returns:
            The entity data.
        """
        return self._wrap_callable(
            lambda: self._transport().request(
                "GET", self._entity_path(entity_id), params=params
            ),
            log_context={"path": self._entity_path(entity_id)},
        )

    def fetch_one(
        self, entity_id: str, *, params: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        """Alias for :meth:`get` to align with generic client interface."""

        return self.get(entity_id, params=params)

    def fetch_by_ids(self, ids: Sequence[str]) -> Iterator[dict[str, Any]]:
        """Retrieve entities by IDs.

        Args:
            ids: The entity IDs.

        Returns:
            An iterator over entity data.
        """
        return self.iter_ids(ids)

    def iterate_records(
        self,
        *,
        ids: Sequence[str] | None = None,
        page_size: int | None = None,
        fetcher: Callable[[Sequence[str] | None], Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Iterate over entity records.

        Args:
            ids: The entity IDs.
            page_size: The page size.
            fetcher: The fetcher function.

        Returns:
            An iterator over entity records.
        """
        return iterate_records(
            ids=ids,
            page_size=page_size,
            fetcher=fetcher,
            fetch_by_ids=self.fetch_by_ids,
            list_entities=lambda: self.list(page_size=page_size or 1000),
            normalize_payload=self._normalize_payload,
            wrap_iterator=self._wrap_iterator,
        )

    def fetch_batch(
        self,
        *,
        ids: Sequence[str],
        page_size: int | None = None,
        fetcher: Callable[[Sequence[str]], Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Explicit batching entrypoint delegating to :meth:`iterate_records`."""

        yield from self.iterate_records(
            ids=ids, page_size=page_size, fetcher=fetcher
        )

    def list(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[dict[str, Any]]:
        """Retrieve a list of entities.

        Args:
            page_size: The page size.
            params: The request parameters.
            page_key: The page key.
            next_key: The next key.
            page_param: The page parameter.

        Returns:
            An iterator over entity data.
        """
        return list_entities(
            transport=self._transport(),
            entity_path=self._entity_path(),
            pagination_strategy=self.pagination_strategy,
            wrap_callable=self._wrap_callable,
            wrap_iterator=self._wrap_iterator,
            normalize_payload=lambda payload: self._normalize_payload(
                payload, page_key=page_key
            ),
            normalize_page=lambda page: self._normalize_payload(
                page, page_key=page_key
            ),
            logger=self._logger,
            page_size=page_size,
            params=params,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )

    @property
    def retry_strategy(self) -> Any:
        strategy = getattr(self._transport(), "retry_strategy", None)
        if strategy is None:
            raise AttributeError("Transport does not expose retry_strategy")
        return strategy

    @property
    def timeout_seconds(self) -> float:
        raw = getattr(self._transport(), "timeout_seconds", None)
        return cast(float, raw) if raw is not None else 0.0

    def fetch_all(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[dict[str, Any]]:
        """Fetch all entities.

        Args:
            page_size: The page size.
            params: The request parameters.
            page_key: The page key.
            next_key: The next key.
            page_param: The page parameter.

        Returns:
            An iterator over entity data.
        """
        return warn_fetch_all(
            list_entities_fn=lambda: self.list(
                page_size=page_size,
                params=params,
                page_key=page_key,
                next_key=next_key,
                page_param=page_param,
            ),
            wrap_iterator=self._wrap_iterator,
        )

    def search(self, params: Mapping[str, Any]) -> Iterator[dict[str, Any]]:
        """Search for entities.

        Args:
            params: The search parameters.

        Returns:
            An iterator over entity data.
        """
        return self.list(params=params)


__all__ = ["BaseApiEntityClient", "EntityClientProtocol"]
