from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import structlog

from bioetl.clients.common import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
    ApiClientMixin,
    ApiTransportProtocol,
    ClosableMixin,
    NextLinkPagination,
    PaginationStrategy,
)
from bioetl.clients.entities._base import _BaseEntityClient
from bioetl.core.pipeline.unified import ChemblExtractionDescriptor


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
    ) -> None:
        super().__init__(
            transport,
            entity,
            pagination_strategy=pagination_strategy or NextLinkPagination(),
        )

    def default_pagination_strategy(self) -> PaginationStrategy:
        return NextLinkPagination()

    def iterate_records(self, descriptor: ChemblExtractionDescriptor) -> Iterator[dict[str, Any]]:
        def iterator() -> Iterator[dict[str, Any]]:
            context: Mapping[str, Any] | None = None
            try:
                context = descriptor.build_context(self)
            except Exception:
                context = None

            ids: Sequence[str] | None = None
            page_size = 1000
            if isinstance(context, Mapping):
                ids_value = context.get("ids")
                if isinstance(ids_value, Sequence) and not isinstance(ids_value, (str, bytes, bytearray)):
                    ids = [str(item) for item in ids_value]
                page_size_value = context.get("page_size")
                if isinstance(page_size_value, int):
                    page_size = page_size_value

            fetcher_factory = getattr(descriptor, "fetcher_factory", None)
            if callable(fetcher_factory):
                fetcher = fetcher_factory(context or {})
                if callable(fetcher):
                    result = fetcher(ids)
                    if isinstance(result, Iterator):
                        yield from result
                        return
                    if isinstance(result, Sequence) and not isinstance(result, (str, bytes, bytearray)):
                        for item in result:
                            if isinstance(item, Mapping):
                                yield dict(item)
                        return
                    if isinstance(result, Mapping):
                        yield dict(result)
                        return

            if ids:
                yield from self.fetch_by_ids(ids)
                return

            yield from self.list(page_size=page_size)

        return self._wrap_iterator(iterator)


__all__ = ["BaseChemblClient", "ChemblEntityClient"]
