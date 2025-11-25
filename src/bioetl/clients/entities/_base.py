from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import structlog

from bioetl.base_classes import BaseApiClient, EntityClientProtocol
from bioetl.clients.common import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
    PageParamPagination,
    PaginatedFetcher,
    iterate_by_ids,
)
from bioetl.core.http.client_mixins import ApiClientMixin, ClosableMixin


class _BaseEntityClient(ClosableMixin, ApiClientMixin, BaseApiClient, EntityClientProtocol):
    def __init__(
        self,
        api_client: BaseApiClient,
        entity: str,
        *,
        pagination_strategy: PaginatedFetcher | None = None,
    ) -> None:
        self.api_client = api_client
        self.entity = entity.strip("/")
        self._logger = structlog.get_logger(__name__).bind(entity=self.entity)
        self.pagination_strategy = pagination_strategy or PageParamPagination()

    def fetch_by_ids(self, ids: Sequence[str]) -> Iterator[dict[str, Any]]:
        return iterate_by_ids(
            ids=ids,
            entity=self.entity,
            api_client=self.api_client,
            normalize=self._normalize_payload,
            wrap_callable=self._wrap_callable,
            wrap_iterator=self._wrap_iterator,
            logger=self._logger,
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
        def iterator() -> Iterator[dict[str, Any]]:
            query_params: dict[str, Any] = {"limit": page_size}
            if params:
                query_params.update(params)

            yield from self.pagination_strategy.paginate(
                self.api_client,
                f"/{self.entity}",
                params=query_params,
                logger=self._logger,
                page_key=page_key,
                next_key=next_key,
                page_param=page_param,
                normalize=self._normalize_payload,
            )

        return self._wrap_iterator(iterator)

    def get_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Mapping[str, Any] | list[Mapping[str, Any]]:
        return self.api_client.get_json(endpoint, params=params, headers=headers)

    def paginate_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[Mapping[str, Any]]:
        return self.api_client.paginate_json(
            endpoint,
            params=params,
            headers=headers,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )
