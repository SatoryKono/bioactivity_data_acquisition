from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import Any

import structlog

from bioetl.clients.utils import pagination as pagination_utils
from bioetl.core.http import ApiClientMixin, ClosableMixin
from bioetl.core.http.interfaces import ApiTransportProtocol
from bioetl.core.http.pagination import PaginationStrategy


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
        return pagination_utils.iter_ids(
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
        return pagination_utils.iterate_records(
            ids=ids,
            page_size=page_size,
            fetcher=fetcher,
            fetch_by_ids=self.fetch_by_ids,
            list_entities=lambda effective_page_size: self.list(
                page_size=effective_page_size or 1000
            ),
            normalize=self._normalize_payload,
            wrap_iterator=self._wrap_iterator,
        )

    def list(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[dict[str, Any]]:
        return pagination_utils.list_entities(
            page_size=page_size,
            params=params,
            pagination_strategy=self.pagination_strategy,
            endpoint=self._entity_path(),
            transport=self._transport(),
            logger=self._logger,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
            normalize=self._normalize_payload,
            wrap_callable=self._wrap_callable,
            wrap_iterator=self._wrap_iterator,
        )

    def fetch_all(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = "results",
        next_key: str = "next",
        page_param: str | None = "page",
    ) -> Iterator[dict[str, Any]]:
        return pagination_utils.warn_fetch_all(
            list_callable=self.list,
            list_kwargs={
                "page_size": page_size,
                "params": params,
                "page_key": page_key,
                "next_key": next_key,
                "page_param": page_param,
            },
        )

    def search(self, params: Mapping[str, Any]) -> Iterator[dict[str, Any]]:
        return self.list(params=params)


__all__ = ["BaseApiEntityClient"]
