from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any, Protocol

import structlog

from bioetl.core.http.client_mixins import ApiClientMixin, ClosableMixin
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
    PaginationStrategy,
)
from bioetl.core.http.pagination_helpers import iter_ids, iterate_records, list_entities, warn_fetch_all
from bioetl.core.http.types import Normalizer

class EntityClientProtocol(Protocol):
    entity: str

    def get(self, entity_id: str, *, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        ...

    def list(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[Mapping[str, Any]]:
        ...

    def fetch_by_ids(self, ids: Sequence[str]) -> Iterator[Mapping[str, Any]]:
        ...

    def search(self, params: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
        ...

    def close(self) -> None:
        ...


class BaseApiEntityClient(ApiClientMixin, ClosableMixin):
    """Базовый клиент сущности API с общей логикой обхода записей."""

    def __init__(self, transport: ApiTransportProtocol, pagination: PaginationStrategy, *, entity: str) -> None:
        self.transport = transport
        self.entity = entity.strip("/")
        self.pagination_strategy = pagination
        self._logger = structlog.get_logger(__name__).bind(entity=self.entity)

    def _entity_path(self, suffix: str | None = None) -> str:
        if not suffix:
            return f"/{self.entity}"
        suffix = str(suffix).lstrip("/")
        return f"/{self.entity}/{suffix}"

    def iter_ids(
        self,
        ids: Sequence[str],
        path_template: str = "/{entity}/{id}",
    ) -> Iterator[dict[str, Any]]:
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

    def get(self, entity_id: str, *, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        return self._wrap_callable(
            lambda: self._transport().request("GET", self._entity_path(entity_id), params=params),
            log_context={"path": self._entity_path(entity_id)},
        )

    def fetch_by_ids(self, ids: Sequence[str]) -> Iterator[dict[str, Any]]:
        return self.iter_ids(ids)

    def iterate_records(
        self,
        *,
        ids: Sequence[str] | None = None,
        page_size: int | None = None,
        fetcher: Callable[[Sequence[str] | None], Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        return iterate_records(
            ids=ids,
            page_size=page_size,
            fetcher=fetcher,
            fetch_by_ids=self.fetch_by_ids,
            list_entities=lambda: self.list(page_size=page_size or 1000),
            normalize_payload=self._normalize_payload,
            wrap_iterator=self._wrap_iterator,
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
        return list_entities(
            transport=self._transport(),
            entity_path=self._entity_path(),
            pagination_strategy=self.pagination_strategy,
            wrap_callable=self._wrap_callable,
            wrap_iterator=self._wrap_iterator,
            normalize_payload=lambda payload: self._normalize_payload(payload, page_key=page_key),
            normalize_page=lambda page: self._normalize_payload(page, page_key=page_key),
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
        return self.list(params=params)


__all__ = ["BaseApiEntityClient", "EntityClientProtocol"]
