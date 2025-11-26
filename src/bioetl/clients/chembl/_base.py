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
    PaginationStrategy,
)
from bioetl.clients.entities._base import _BaseEntityClient
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

    def iterate_records(self, descriptor: ChemblExtractionDescriptor) -> Iterator[dict[str, Any]]:
        def iterator() -> Iterator[dict[str, Any]]:
            try:
                context = descriptor.build_context(self)
            except Exception:
                context = None

            context_mapping: Mapping[str, Any] = context if isinstance(context, Mapping) else {}
            ids = self._extract_ids(context_mapping)
            page_size = self._resolve_page_size(context_mapping)

            fetcher_factory = getattr(descriptor, "fetcher_factory", None)
            fetcher = fetcher_factory(context_mapping) if callable(fetcher_factory) else None

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

            yield from self.list(page_size=page_size)

        return self._wrap_iterator(iterator)

    @staticmethod
    def _extract_ids(context: Mapping[str, Any]) -> list[str] | None:
        ids = context.get("ids")
        if isinstance(ids, Sequence) and not isinstance(ids, (str, bytes, bytearray)):
            return [str(item) for item in ids]
        return None

    @staticmethod
    def _resolve_page_size(context: Mapping[str, Any], default: int = 1000) -> int:
        page_size = context.get("page_size")
        if isinstance(page_size, int):
            return page_size
        return default


__all__ = ["BaseChemblClient", "ChemblEntityClient"]
