"""Core pagination strategies shared by source adapters."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from bioetl.core.api_client import UnifiedAPIClient
Parser = Callable[[Any], Sequence[Mapping[str, Any]]]
Extractor = Callable[[Mapping[str, Any]], Sequence[Mapping[str, Any]]]
CursorExtractor = Callable[[Mapping[str, Any]], str | None]
InvalidPayloadHandler = Callable[[int], None]
EmptyPageHandler = Callable[[int], None]
PageLimitHandler = Callable[[int], None]


def _coerce_batch(items: Sequence[Mapping[str, Any]] | None) -> list[dict[str, Any]]:
    """Return a list of plain dictionaries filtered from ``items``."""

    batch: list[dict[str, Any]] = []
    if items is None:
        return batch

    for item in items:
        if isinstance(item, Mapping):
            batch.append(dict(item))
    return batch


@dataclass(slots=True)
class PageNumberPaginationStrategy:
    """Paginate APIs that expose explicit page numbers."""

    client: UnifiedAPIClient
    page_param: str = "page"
    page_size_param: str = "pageSize"
    offset_param: str | None = "offset"
    limit_param: str | None = "limit"

    def collect(
        self,
        path: str,
        *,
        page_size: int,
        unique_key: str,
        parser: Parser,
        params: Mapping[str, Any] | None = None,
        start_page: int = 1,
    ) -> list[dict[str, Any]]:
        """Collect every page until the API returns an empty batch."""

        if page_size <= 0:
            msg = "page_size must be a positive integer"
            raise ValueError(msg)

        collected: list[dict[str, Any]] = []
        seen: set[Any] = set()
        page = start_page

        while True:
            query: dict[str, Any] = dict(params or {})
            query.setdefault(self.page_param, page)
            if self.page_size_param:
                query.setdefault(self.page_size_param, page_size)
            if self.offset_param is not None:
                query.setdefault(self.offset_param, (page - 1) * page_size)
            if self.limit_param is not None:
                query.setdefault(self.limit_param, page_size)

            payload = self.client.request_json(path, params=query)
            items = parser(payload)
            batch: list[dict[str, Any]] = []
            for item in items:
                if not isinstance(item, Mapping):
                    continue
                key_value = item.get(unique_key)
                if key_value is not None:
                    if key_value in seen:
                        continue
                    seen.add(key_value)
                batch.append(dict(item))

            if not batch:
                break

            collected.extend(batch)

            if len(batch) < page_size:
                break

            page += 1

        return collected


@dataclass(slots=True)
class OffsetPaginationStrategy:
    """Paginate APIs that expose offset/limit style parameters."""

    client: UnifiedAPIClient
    offset_param: str = "offset"
    limit_param: str = "limit"

    def collect(
        self,
        path: str,
        *,
        page_size: int,
        extract_items: Extractor,
        params: Mapping[str, Any] | None = None,
        max_items: int | None = None,
    ) -> list[dict[str, Any]]:
        """Collect every batch until the API reports exhaustion."""

        if page_size <= 0:
            msg = "page_size must be a positive integer"
            raise ValueError(msg)

        collected: list[dict[str, Any]] = []
        offset = 0

        while True:
            query: dict[str, Any] = dict(params or {})
            if self.limit_param:
                query[self.limit_param] = page_size
            if self.offset_param:
                query[self.offset_param] = offset

            payload = self.client.request_json(path, params=query)
            if not isinstance(payload, Mapping):
                break

            batch = _coerce_batch(extract_items(payload))
            if not batch:
                break

            collected.extend(batch)

            if max_items is not None and len(collected) >= max_items:
                return collected[:max_items]

            if len(batch) < page_size:
                break

            offset += page_size

        return collected


@dataclass(slots=True)
class CursorPaginationStrategy:
    """Paginate APIs that use cursor continuation tokens."""

    client: UnifiedAPIClient
    cursor_param: str = "cursor"
    page_size_param: str = "per_page"

    def collect(
        self,
        path: str,
        *,
        page_size: int,
        unique_key: str,
        parse_items: Extractor,
        extract_cursor: CursorExtractor,
        params: Mapping[str, Any] | None = None,
        max_pages: int | None = None,
        on_invalid_payload: InvalidPayloadHandler | None = None,
        on_empty_page: EmptyPageHandler | None = None,
        on_page_limit: PageLimitHandler | None = None,
    ) -> list[dict[str, Any]]:
        """Collect paginated results using opaque cursor tokens."""

        if page_size <= 0:
            msg = "page_size must be a positive integer"
            raise ValueError(msg)

        collected: list[dict[str, Any]] = []
        seen: set[str] = set()
        cursor: str | None = None
        page_index = 0

        while True:
            if max_pages is not None and page_index >= max_pages:
                if on_page_limit is not None:
                    on_page_limit(page_index)
                break

            query: dict[str, Any] = dict(params or {})
            if self.page_size_param:
                query.setdefault(self.page_size_param, page_size)
            if cursor:
                query[self.cursor_param] = cursor

            payload = self.client.request_json(path, params=query)
            if not isinstance(payload, Mapping):
                if on_invalid_payload is not None:
                    on_invalid_payload(page_index)
                break

            page_index += 1

            batch: list[dict[str, Any]] = []
            for item in parse_items(payload):
                if not isinstance(item, Mapping):
                    continue
                key_value = item.get(unique_key)
                if key_value is None:
                    continue
                key_str = str(key_value).strip()
                if not key_str or key_str in seen:
                    continue
                seen.add(key_str)
                batch.append(dict(item))

            if not batch:
                if on_empty_page is not None:
                    on_empty_page(page_index)
                break

            collected.extend(batch)

            next_cursor = extract_cursor(payload)
            if not next_cursor:
                break

            cursor = next_cursor

        return collected


TState = TypeVar("TState")


@dataclass(slots=True)
class TokenInitialization(Generic[TState]):
    """Describe the initial state for token-based pagination."""

    state: TState
    total_count: int


@dataclass(slots=True)
class TokenPaginationStrategy(Generic[TState]):
    """Paginate APIs that require an initialization token exchange."""

    client: UnifiedAPIClient

    def collect(
        self,
        *,
        initializer: Callable[[UnifiedAPIClient], TokenInitialization[TState] | None],
        fetcher: Callable[[UnifiedAPIClient, TState, int, int], Sequence[Mapping[str, Any]] | None],
        batch_size: int,
    ) -> list[dict[str, Any]]:
        """Collect paginated records after exchanging continuation tokens."""

        if batch_size <= 0:
            msg = "batch_size must be a positive integer"
            raise ValueError(msg)

        init = initializer(self.client)
        if init is None or init.total_count <= 0:
            return []

        state = init.state
        total_count = init.total_count
        offset = 0
        collected: list[dict[str, Any]] = []

        while offset < total_count:
            chunk_size = min(batch_size, total_count - offset)
            items = fetcher(self.client, state, offset, chunk_size) or []
            batch = _coerce_batch(items)
            if not batch:
                break

            collected.extend(batch)
            offset += chunk_size

        return collected
