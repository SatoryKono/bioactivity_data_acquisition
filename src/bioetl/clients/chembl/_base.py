from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Callable

import structlog

from bioetl.clients.common import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
    ApiTransportProtocol,
    PaginationStrategy,
)
from bioetl.clients.entities._base import _BaseEntityClient
from bioetl.core.http.client_mixins import ApiClientMixin, ClosableMixin
from bioetl.core.pipeline.unified import ChemblExtractionDescriptor
from bioetl.infra import PaginationRegistry


class BaseChemblClient(ApiClientMixin, ClosableMixin, ApiTransportProtocol):
    """Транспортный клиент ChEMBL без привязки к конкретной сущности."""

    def __init__(self, transport: ApiTransportProtocol) -> None:
        self.transport = transport
        self._logger = structlog.get_logger(__name__).bind(client="chembl_transport")

    def request(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json: Any | None = None,
    ) -> Mapping[str, Any] | Sequence[Mapping[str, Any]]:
        return self._wrap_callable(
            lambda: self.transport.request(
                method,
                path,
                headers=headers,
                params=params,
                json=json,
            ),
            log_context={"path": path, "method": method},
        )


class ChemblEntityClient(_BaseEntityClient):
    def __init__(
        self,
        transport: ApiTransportProtocol,
        entity: str,
        *,
        pagination_strategy: PaginationStrategy | None = None,
        pagination_strategy_name: str | None = None,
        pagination_registry: PaginationRegistry | None = None,
    ) -> None:
        super().__init__(
            transport,
            entity,
            pagination_strategy=pagination_strategy,
            pagination_strategy_name=pagination_strategy_name,
            pagination_registry=pagination_registry,
        )

    def default_pagination_strategy(self, *, strategy_name: str | None = None) -> PaginationStrategy:
        return self.pagination_registry.create(strategy_name or "next_link")

    def iterate_records(
        self,
        *,
        ids: Sequence[str] | None = None,
        page_size: int | None = None,
        fetcher: Callable[[Sequence[str] | None], Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        def iterator() -> Iterator[dict[str, Any]]:
            if callable(fetcher):
                result = fetcher(ids)
                if isinstance(result, Iterator):
                    yield from result
                    return
                if result is not None:
                    yield from self._normalize_payload(result)
                    return

            if ids:
                yield from self.fetch_by_ids(ids)
                return

            yield from self.list(page_size=page_size or 1000)

        return self._wrap_iterator(iterator)


__all__ = ["BaseChemblClient", "ChemblEntityClient"]
