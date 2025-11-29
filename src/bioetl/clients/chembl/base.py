from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Protocol, cast

from bioetl.clients.base import (
    DataProviderProtocol,
    Page,
    PageStream,
    PaginationParams,
    RecordStream,
    RequestContext,
)
from bioetl.clients.chembl.strategy_resolver import PaginationStrategyResolverMixin
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
    PaginationStrategy,
)
from bioetl.core.http.pagination_helpers import normalize_payload


class ChemblClientProtocol(Protocol):
    """Contract for ChEMBL entity clients."""

    entity: str

    def get(
        self, entity_id: str, *, params: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        ...

    def fetch_one(
        self, entity_id: str, *, params: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
        ...

    def fetch_many(
        self,
        *,
        query: Mapping[str, Any] | None = None,
        page_size: int | None = None,
        params: Mapping[str, Any] | None = None,
        pagination: PaginationParams | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> RecordStream:
        ...

    def fetch_batch(
        self,
        ids: Sequence[str],
        *,
        params: Mapping[str, Any] | None = None,
        path_template: str = "/{entity}/{id}",
    ) -> RecordStream:
        ...

    def status(self) -> Mapping[str, Any]:
        ...

    def metadata(self) -> Mapping[str, Any]:
        ...

    def close(self) -> None:
        ...


class BaseChemblEntityProtocol(ChemblClientProtocol, Protocol):
    """Alias for typed ChEMBL entity clients."""


class BaseChemblClient(
    PaginationStrategyResolverMixin,
    DataProviderProtocol[dict[str, Any]],
    ChemblClientProtocol,
):
    """Тонкая обёртка над транспортом ChEMBL без бизнес-логики."""

    def __init__(
        self,
        transport: ApiTransportProtocol,
        entity: str,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_factories: Mapping[str, Any] | None = None,
    ) -> None:
        self._transport = transport
        self.entity = entity.strip("/")
        self.pagination_strategy = self.resolve_strategy(
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

    def _entity_path(self, suffix: str | None = None) -> str:
        if not suffix:
            return f"/{self.entity}"
        return f"/{self.entity}/{suffix.lstrip('/')}"

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

    def fetch_one(
        self,
        ref: str,
        *,
        params: Mapping[str, Any] | None = None,
        context: RequestContext | None = None,
    ) -> RecordStream:
        _ = context
        payload = self._transport.request(
            "GET", self._entity_path(ref), params=params
        )
        yield from normalize_payload(payload, page_key=None)

    def fetch_batch(
        self,
        ids: Sequence[str],
        *,
        params: Mapping[str, Any] | None = None,
        path_template: str = "/{entity}/{id}",
        context: RequestContext | None = None,
    ) -> RecordStream:
        _ = context
        for entity_id in ids:
            path = path_template.format(entity=self.entity, id=entity_id)
            payload = self._transport.request("GET", path, params=params)
            yield from normalize_payload(payload, page_key=None)

    def fetch_many(
        self,
        *,
        query: Mapping[str, Any] | None = None,
        page_size: int | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> RecordStream:
        _ = context
        effective_pagination = self._resolve_pagination(
            pagination,
            fallback_page_size=page_size,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )
        merged_params: dict[str, Any] = {}
        if query:
            merged_params.update(query)
        if params:
            merged_params.update(params)
        response = self._transport.request(
            "GET", self._entity_path(), params=merged_params or None
        )
        strategy = self.pagination_strategy
        if strategy is None:
            yield from normalize_payload(response, page_key=effective_pagination.page_key)
            return

        for page in strategy.iter_pages(
            response,
            self._transport,
            endpoint=self._entity_path(),
            params=merged_params or None,
            page_key=effective_pagination.page_key,
            next_key=effective_pagination.next_key,
            page_param=effective_pagination.page_param,
            normalize=None,
        ):
            yield from normalize_payload(page, page_key=effective_pagination.page_key)

    def iter_pages(
        self,
        *,
        query: Mapping[str, Any] | None = None,
        pagination: PaginationParams | None = None,
        context: RequestContext | None = None,
    ) -> PageStream:
        _ = context
        effective_pagination = self._resolve_pagination(pagination)
        params = dict(query or {})
        if effective_pagination.page_size:
            params.setdefault("limit", effective_pagination.page_size)

        response = self._transport.request(
            "GET", self._entity_path(), params=params or None
        )
        strategy = self.pagination_strategy
        if strategy is None:
            items = list(normalize_payload(response, page_key=effective_pagination.page_key))
            yield Page(items=items, next_cursor=None, raw=cast(Mapping[str, Any] | None, response if isinstance(response, Mapping) else None))
            return

        for raw_page in strategy.iter_pages(
            response,
            self._transport,
            endpoint=self._entity_path(),
            params=params or None,
            page_key=effective_pagination.page_key,
            next_key=effective_pagination.next_key,
            page_param=effective_pagination.page_param,
            normalize=None,
        ):
            items = list(
                normalize_payload(raw_page, page_key=effective_pagination.page_key)
            )
            next_cursor: int | str | None = None
            if isinstance(raw_page, Mapping):
                next_cursor = cast(int | str | None, raw_page.get(effective_pagination.next_key or DEFAULT_NEXT_KEY))
            yield Page(
                items=items,
                next_cursor=next_cursor,
                raw=raw_page if isinstance(raw_page, Mapping) else None,
            )

    def status(self) -> Mapping[str, Any]:
        result = self._transport.request("GET", "/status")
        return result if isinstance(result, Mapping) else {}

    def metadata(self) -> Mapping[str, Any]:
        meta = getattr(self._transport, "metadata", None)
        return dict(meta) if isinstance(meta, Mapping) else {}

    def get(
        self, entity_id: str, *, params: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        result = self._transport.request(
            "GET", self._entity_path(entity_id), params=params
        )
        return cast(Mapping[str, Any], result)

    def iterate_records(
        self,
        *,
        ids: Sequence[str] | None = None,
        page_size: int | None = None,
        fetcher: Any | None = None,
    ) -> Iterator[dict[str, Any]]:
        _ = fetcher
        if ids:
            yield from self.fetch_batch(ids, params=None)
        else:
            yield from self.fetch_many(page_size=page_size)

    def close(self) -> None:
        close = getattr(self._transport, "close", None)
        if callable(close):
            close()


class ChemblEntityClient(BaseChemblClient):
    """Client for specific ChEMBL entities."""

    def __init__(
        self,
        transport: ApiTransportProtocol,
        entity: str,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_factories: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            transport,
            entity,
            pagination_strategy=pagination_strategy,
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
