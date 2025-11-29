"""Base ChEMBL client implementations."""
from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Callable, Protocol, cast

from bioetl.clients.chembl.adapter import ChemblTransportAdapter
from bioetl.clients.chembl.compat import ChemblCompatibilityMixin
from bioetl.clients.chembl.pagination import (
    PaginationFactory,
)
from bioetl.clients.chembl.strategy_resolver import (
    PaginationStrategyResolverMixin,
)
from bioetl.clients.base import (
    DataProviderProtocol,
    Page,
    PageStream,
    PaginationParams,
    RecordStream,
    RequestContext,
)
from bioetl.clients.base import exceptions as provider_exceptions
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
    PaginationStrategyResolverMixin,
    ChemblCompatibilityMixin,
    BaseApiEntityClient,
    ChemblClientProtocol,
    DataProviderProtocol[dict[str, Any]],
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
        strategy = self.resolve_strategy(
            transport,
            name=pagination_strategy_name,
            factories=pagination_factories,
            default=pagination_strategy,
        )
        self._default_pagination = PaginationParams(
            page_key=DEFAULT_PAGE_KEY,
            next_key=DEFAULT_NEXT_KEY,
            page_param=DEFAULT_PAGE_PARAM,
        )
        super().__init__(transport, strategy, entity=entity)

    def configure(
        self,
        *,
        transport: Any | None = None,
        pagination: PaginationParams | None = None,
        retries: Any | None = None,
    ) -> "BaseChemblClient":
        _ = (transport, retries)
        if pagination is not None:
            self._default_pagination = pagination
        return self

    def fetch_batch(
        self,
        ids: Sequence[str],
        *,
        params: Mapping[str, Any] | None = None,
        path_template: str = "/{entity}/{id}",
        context: RequestContext | None = None,
    ) -> RecordStream:
        """Fetch multiple entities by IDs using the base implementation."""

        _ = context
        return super().fetch_batch(
            ids, params=params, path_template=path_template
        )

    def _resolve_pagination(
        self,
        pagination: PaginationParams | None,
        *,
        fallback_page_size: int | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> PaginationParams:
        base_params = self._default_pagination.override(
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
            page_size=fallback_page_size,
        )
        return base_params.override(
            page_key=pagination.page_key if pagination else None,
            next_key=pagination.next_key if pagination else None,
            page_param=pagination.page_param if pagination else None,
            page_size=pagination.page_size if pagination else None,
        )

    def fetch_many(
        self,
        *,
        query: Mapping[str, Any] | None = None,
        page_size: int | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:
        """Stream normalized records for the given query.

        Args:
            query: Набор фильтров/параметров ChEMBL.
            page_size: Желаемый размер страницы (``limit`` в ChEMBL).
            pagination: Пользовательские параметры пагинации.
            context: Дополнительный контекст запроса.

        Returns:
            Итератор словарей с нормализованными записями.
        """

        _ = context  # контекст используется только для логирования выше по стеку

        effective_pagination = self._resolve_pagination(
            pagination,
            fallback_page_size=page_size or 1000,
        )
        params: dict[str, Any] = dict(query or {})
        if effective_pagination.page_size:
            params.setdefault("limit", effective_pagination.page_size)

        base_fetch_many = super().fetch_many

        def iterator() -> Iterator[dict[str, Any]]:
            try:
                yield from base_fetch_many(
                    page_size=effective_pagination.page_size or 1000,
                    params=params or None,
                    page_key=effective_pagination.page_key or DEFAULT_PAGE_KEY,
                    next_key=effective_pagination.next_key or DEFAULT_NEXT_KEY,
                    page_param=effective_pagination.page_param,
                )
            except Exception as exc:  # noqa: BLE001
                raise provider_exceptions.ProviderError(str(exc)) from exc

        return iterator()

    def iter_pages(
        self,
        *,
        query: Mapping[str, Any] | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> PageStream:
        """Iterate over pages while applying ChEMBL pagination rules."""

        effective_pagination = self._resolve_pagination(
            pagination,
            fallback_page_size=pagination.page_size if pagination else None,
        )
        params = dict(query or {})
        if effective_pagination.page_size:
            params.setdefault("limit", effective_pagination.page_size)

        entity_path = self._entity_path()
        log_context: dict[str, Any] = {"path": entity_path}
        if context:
            log_context.update(context.extra)

        try:
            first_payload = self._wrap_callable(
                lambda: cast(ApiTransportProtocol, self._transport()).request(
                    "GET", entity_path, params=params
                ),
                log_context=log_context,
            )
            strategy = cast(PaginationStrategy, self.pagination_strategy)
            page_key = effective_pagination.page_key or DEFAULT_PAGE_KEY
            next_key = effective_pagination.next_key or DEFAULT_NEXT_KEY
            page_param = effective_pagination.page_param

            for raw_page in strategy.iter_pages(
                first_payload,
                cast(ApiTransportProtocol, self._transport()),
                endpoint=entity_path,
                params=params,
                logger=self._logger,
                page_key=page_key,
                next_key=next_key,
                page_param=page_param,
                normalize=self._normalize_payload,
            ):
                items = list(self._normalize_payload(raw_page, page_key=page_key))
                next_cursor = (
                    raw_page.get(next_key)
                    if isinstance(raw_page, Mapping)
                    else None
                )
                yield Page(
                    items=items,
                    next_cursor=next_cursor,
                    raw=raw_page if isinstance(raw_page, Mapping) else None,
                )
        except Exception as exc:  # noqa: BLE001
            raise provider_exceptions.ProviderError(str(exc)) from exc

    def metadata(self) -> Mapping[str, Any]:
        """Return metadata describing the client and underlying transport."""

        base_transport = getattr(self, "transport", None)
        transport_meta: dict[str, Any] = {}
        if base_transport is not None:
            metadata = getattr(base_transport, "metadata", None)
            if isinstance(metadata, Mapping):
                transport_meta.update(metadata)

        return {
            "source": "chembl",
            "entity": self.entity,
            "transport": transport_meta,
        }

    def status(self) -> Mapping[str, Any]:
        """Check ChEMBL API status."""
        result = self._wrap_callable(
            lambda: self._transport().request("GET", "/status"),
            log_context={"path": "/status", "method": "GET"},
        )
        # Ensure we return Mapping[str, Any] as expected
        if isinstance(result, Mapping):
            return result
        return {}

    def fetch_one(
        self,
        ref: str,
        *,
        params: Mapping[str, Any] | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:
        """Fetch a single entity and yield normalized record(s)."""

        log_context = {"ref": ref}
        if context:
            log_context.update(context.extra)

        wrapped_iterator = self._wrap_iterator(
            lambda: self._normalize_payload(self.get(ref, params=params), page_key=None),
            log_context=log_context,
        )

        def provider_iterator() -> Iterator[dict[str, Any]]:
            try:
                yield from wrapped_iterator
            except Exception as exc:  # noqa: BLE001
                raise provider_exceptions.ProviderError(str(exc)) from exc

        return provider_iterator()

    def close(self) -> None:
        """Close underlying transport or propagate a provider-level error."""

        try:
            super().close()
        except Exception as exc:  # noqa: BLE001
            raise provider_exceptions.ProviderError(str(exc)) from exc


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
            pagination_strategy_name=pagination_strategy_name,
            pagination_factories=pagination_factories,
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
