from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import structlog

from bioetl.base_classes import BaseApiClient
from bioetl.clients.common import ApiClientMixin, PageParamPagination, PaginationStrategy


class _BaseEntityClient(ApiClientMixin):
    def __init__(
        self,
        api_client: BaseApiClient,
        entity: str,
        *,
        pagination_strategy: PaginationStrategy | None = None,
    ) -> None:
        self.api_client = api_client
        self.entity = entity.strip("/")
        self._logger = structlog.get_logger(__name__).bind(entity=self.entity)
        self.pagination_strategy = pagination_strategy or PageParamPagination()

    def fetch_by_ids(self, ids: Sequence[str]) -> Iterator[dict[str, Any]]:
        return self.iter_ids(ids, "/{entity}/{id}")

    def fetch_all(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        def iterator() -> Iterator[dict[str, Any]]:
            query_params: dict[str, Any] = {"limit": page_size}
            if params:
                query_params.update(params)

            for payload in self.pagination_strategy.paginate(
                self.api_client,
                f"/{self.entity}",
                params=query_params,
                logger=self._logger,
            ):
                yield from self._normalize_payload(payload)

        return self._wrap_iterator(iterator)
