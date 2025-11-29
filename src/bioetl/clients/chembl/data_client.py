"""Адаптеры для работы с ChEMBL через новый контракт ``DataClient``."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from bioetl.clients import ClientRequest, DataClient, Page, PageStream, Record, RecordStream
from bioetl.clients.base.client import PaginationParams, RequestContext
from bioetl.clients.chembl.factories import default_chembl_factory


def _apply_limit(iterator: Iterable[Record], limit: int | None) -> RecordStream:
    if limit is None:
        yield from iterator
        return

    remaining = limit
    for item in iterator:
        if remaining <= 0:
            break
        remaining -= 1
        yield item


@dataclass
class ChemblDataClient(DataClient):
    """Обёртка над существующим ChEMBL-клиентом в терминах ``DataClient``."""

    name: str
    source: str
    _delegate: Any
    _default_page_size: int = 1000

    def fetch_one(
        self, request: ClientRequest, *, context: RequestContext | None = None
    ) -> Record | None:
        _ = context
        params = dict(request.filters or {})
        ids = list(request.ids or [])
        if ids:
            return self._delegate.fetch_one(ids[0], params=params)

        iterator = self.iter_records(request)
        return next(iter(iterator), None)

    def iter_records(
        self, request: ClientRequest, *, context: RequestContext | None = None
    ) -> RecordStream:
        _ = context
        params = dict(request.filters or {})
        pagination = request.pagination or PaginationParams()
        page_size = pagination.page_size or self._default_page_size

        if request.ids:
            iterator = self._delegate.fetch_batch(request.ids, params=params)
        else:
            iterator = self._delegate.fetch_many(page_size=page_size, params=params)

        return _apply_limit(iterator, pagination.limit)

    def iter_pages(
        self, request: ClientRequest, *, context: RequestContext | None = None
    ) -> PageStream:
        _ = context
        pagination = request.pagination or PaginationParams()
        page_size = pagination.page_size or self._default_page_size
        buffer: list[Record] = []
        iterator = iter(self.iter_records(request))

        while True:
            try:
                while len(buffer) < page_size:
                    buffer.append(next(iterator))
            except StopIteration:
                if buffer:
                    yield Page(items=list(buffer), has_next=False)
                break

            next_item: Record | None = None
            try:
                next_item = next(iterator)
            except StopIteration:
                next_item = None

            has_next = next_item is not None
            yield Page(items=list(buffer), has_next=has_next)
            buffer = [] if next_item is None else [next_item]

    def close(self) -> None:
        close = getattr(self._delegate, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> "ChemblDataClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        self.close()


def build_chembl_client_factory(
    config: Mapping[str, Any] | Any,
    *,
    name_builder: Callable[[str], str] | None = None,
) -> Callable[[str], DataClient]:
    """Собрать фабрику ``DataClient`` для ChEMBL-ресурсов."""

    legacy_factory = default_chembl_factory(config)
    build_name = name_builder or (lambda entity: f"chembl.{entity}")

    def factory(client_name: str) -> DataClient:
        delegate = legacy_factory.create(client_name)
        return ChemblDataClient(
            name=build_name(client_name),
            source="chembl",
            _delegate=delegate,
        )

    return factory


__all__ = ["ChemblDataClient", "build_chembl_client_factory"]
