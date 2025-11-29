"""Base API entity client implementation."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, Protocol, cast
import warnings

import structlog
from typing_extensions import TypeAlias

from bioetl.core.http.client_mixins import (
    ApiClientMixin,
    ClosableMixin,
)
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

WrapCallable: TypeAlias = Callable[[Callable[[], Any]], Any]
WrapIterator: TypeAlias = Callable[[Iterator[Any]], Iterator[Any]]


class EntityClientProtocol(Protocol):
    """Protocol for entity clients."""

    entity: str

    def get(
        self, entity_id: str, *, params: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        """Retrieve an entity by ID."""

    def fetch_one(
        self, entity_id: str, *, params: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        """Retrieve a single entity."""

    def list(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[Mapping[str, Any]]:
        """Retrieve a list of entities (deprecated)."""

    def fetch_many(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[Mapping[str, Any]]:
        """Retrieve a paginated iterator of entities."""

    def fetch_by_ids(
        self, ids: Sequence[str]
    ) -> Iterator[Mapping[str, Any]]:
        """Retrieve entities by IDs (deprecated alias)."""

    def search(
        self, params: Mapping[str, Any]
    ) -> Iterator[Mapping[str, Any]]:
        """Search for entities."""

    def iterate_records(
        self,
        *,
        ids: Sequence[str] | None = None,
        page_size: int | None = None,
        fetcher: Callable[[Sequence[str] | None], Any] | None = None,
    ) -> Iterator[Mapping[str, Any]]:
        """Yield normalized records, optionally using ``ids`` or a custom
        fetcher."""

    def close(self) -> None:
        """Close the client."""


class BaseApiEntityClient(ApiClientMixin, ClosableMixin):
    """Базовый клиент сущности API с общей логикой
    обхода записей."""

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
        self._pagination_strategy = pagination
        self._logger = structlog.get_logger(__name__).bind(entity=self.entity)

    @property
    def pagination_strategy(self) -> PaginationStrategy:
        """Return the configured pagination strategy."""
        return self._pagination_strategy

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
        *,
        params: Mapping[str, Any] | None = None,
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
            transport=cast(ApiTransportProtocol, self._transport()),
            normalize=self._normalize_payload,
            wrap_callable=cast(
                WrapCallable, self._wrap_callable
            ),  # type: ignore[arg-type]
            wrap_iterator=cast(
                WrapIterator, self._wrap_iterator
            ),  # type: ignore
            logger=self._logger,
            path_template=path_template,
            params=params,
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
        return cast(
            Mapping[str, Any],
            self._wrap_callable(
                lambda: cast(ApiTransportProtocol, self._transport()).request(
                    "GET", self._entity_path(entity_id), params=params
                ),
                log_context={"path": self._entity_path(entity_id)},
            )
        )

    def fetch_by_ids(self, ids: Sequence[str]) -> Iterator[dict[str, Any]]:
        """Retrieve entities by IDs.

        Deprecated:
            This alias is kept for backwards compatibility; prefer
            :meth:`fetch_batch` instead.

        Args:
            ids: The entity IDs.

        Returns:
            An iterator over entity data.
        """
        warnings.warn(
            "fetch_by_ids is deprecated; use fetch_batch instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch_batch(ids)

    def fetch_batch(
        self,
        ids: Sequence[str],
        *,
        params: Mapping[str, Any] | None = None,
        path_template: str = "/{entity}/{id}",
    ) -> Iterator[dict[str, Any]]:
        """Retrieve multiple entities by IDs.

        Args:
            ids: The entity IDs.
            params: Optional query parameters to include with each request.
            path_template: Template for building the entity URL.

        Returns:
            An iterator over entity data.
        """

        return self.iter_ids(ids, path_template=path_template, params=params)

    def fetch_one(
        self, entity_id: str, *, params: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        """Retrieve a single entity by ID.

        Args:
            entity_id: The entity ID.
            params: Optional request parameters.

        Returns:
            The entity payload.
        """
        return self.get(entity_id, params=params)

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
            fetch_by_ids=self.fetch_batch,
            list_entities=lambda: self.fetch_many(
                page_size=page_size or 1000
            ),
            normalize_payload=self._normalize_payload,
            wrap_iterator=cast(
                WrapIterator, self._wrap_iterator
            ),  # type: ignore
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
        """Deprecated alias for fetch_many.

        Prefer :meth:`fetch_many` for new code.
        """
        warnings.warn(
            "list is deprecated; use fetch_many instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch_many(
            page_size=page_size,
            params=params,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
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
        pagination_strategy = cast(
            PaginationStrategy,
            self.pagination_strategy
            or getattr(self.transport, "pagination_strategy", None)
        )
        return list_entities(
            transport=cast(ApiTransportProtocol, self._transport()),
            entity_path=self._entity_path(),
            pagination_strategy=(
                pagination_strategy
            ),
            wrap_callable=cast(
                WrapCallable, self._wrap_callable
            ),  # type: ignore[arg-type]
            wrap_iterator=cast(
                WrapIterator, self._wrap_iterator
            ),  # type: ignore
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
            list_entities_fn=lambda: self.fetch_many(
                page_size=page_size,
                params=params,
                page_key=page_key,
                next_key=next_key,
                page_param=page_param,
            ),
            wrap_iterator=cast(
                WrapIterator, self._wrap_iterator
            ),  # type: ignore
        )

    def search(self, params: Mapping[str, Any]) -> Iterator[dict[str, Any]]:
        """Search for entities.

        Args:
            params: The search parameters.

        Returns:
            An iterator over entity data.
        """
        return self.fetch_many(params=params)


__all__ = ["BaseApiEntityClient", "EntityClientProtocol"]
