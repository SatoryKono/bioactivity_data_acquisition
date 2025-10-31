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


@dataclass(slots=True)
class PageNumberRequest:
    """Request payload for :class:`PageNumberPaginationStrategy`."""

    path: str
    page_size: int
    unique_key: str
    parser: Parser
    params: Mapping[str, Any] | None = None
    start_page: int = 1


@dataclass(slots=True)
class OffsetRequest:
    """Request payload for :class:`OffsetPaginationStrategy`."""

    path: str
    page_size: int
    extract_items: Extractor
    params: Mapping[str, Any] | None = None
    max_items: int | None = None


@dataclass(slots=True)
class CursorRequest:
    """Request payload for :class:`CursorPaginationStrategy`."""

    path: str
    page_size: int
    unique_key: str
    parse_items: Extractor
    extract_cursor: CursorExtractor
    params: Mapping[str, Any] | None = None
    max_pages: int | None = None
    on_invalid_payload: InvalidPayloadHandler | None = None
    on_empty_page: EmptyPageHandler | None = None
    on_page_limit: PageLimitHandler | None = None


TState = TypeVar("TState")


@dataclass(slots=True)
class TokenRequest(Generic[TState]):
    """Request payload for :class:`TokenPaginationStrategy`."""

    initializer: Callable[[UnifiedAPIClient], "TokenInitialization[TState]" | None]
    fetcher: Callable[[UnifiedAPIClient, TState, int, int], Sequence[Mapping[str, Any]] | None]
    batch_size: int


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

    def paginate(self, request: PageNumberRequest) -> list[dict[str, Any]]:
        """Collect every page until the API returns an empty batch."""

        if request.page_size <= 0:
            msg = "page_size must be a positive integer"
            raise ValueError(msg)

        collected: list[dict[str, Any]] = []
        seen: set[Any] = set()
        page = request.start_page

        while True:
            query: dict[str, Any] = dict(request.params or {})
            query.setdefault(self.page_param, page)
            if self.page_size_param:
                query.setdefault(self.page_size_param, request.page_size)
            if self.offset_param is not None:
                query.setdefault(self.offset_param, (page - 1) * request.page_size)
            if self.limit_param is not None:
                query.setdefault(self.limit_param, request.page_size)

            payload = self.client.request_json(request.path, params=query)
            items = request.parser(payload)
            batch: list[dict[str, Any]] = []
            for item in items:
                if not isinstance(item, Mapping):
                    continue
                key_value = item.get(request.unique_key)
                if key_value is not None:
                    if key_value in seen:
                        continue
                    seen.add(key_value)
                batch.append(dict(item))

            if not batch:
                break

            collected.extend(batch)

            if len(batch) < request.page_size:
                break

            page += 1

        return collected

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
        """Backward compatible wrapper around :meth:`paginate`."""

        request = PageNumberRequest(
            path=path,
            page_size=page_size,
            unique_key=unique_key,
            parser=parser,
            params=params,
            start_page=start_page,
        )
        return self.paginate(request)


@dataclass(slots=True)
class OffsetPaginationStrategy:
    """Paginate APIs that expose offset/limit style parameters."""

    client: UnifiedAPIClient
    offset_param: str = "offset"
    limit_param: str = "limit"

    def paginate(self, request: OffsetRequest) -> list[dict[str, Any]]:
        """Collect every batch until the API reports exhaustion."""

        if request.page_size <= 0:
            msg = "page_size must be a positive integer"
            raise ValueError(msg)

        collected: list[dict[str, Any]] = []
        offset = 0

        while True:
            query: dict[str, Any] = dict(request.params or {})
            if self.limit_param:
                query[self.limit_param] = request.page_size
            if self.offset_param:
                query[self.offset_param] = offset

            payload = self.client.request_json(request.path, params=query)
            if not isinstance(payload, Mapping):
                break

            batch = _coerce_batch(request.extract_items(payload))
            if not batch:
                break

            collected.extend(batch)

            if request.max_items is not None and len(collected) >= request.max_items:
                return collected[: request.max_items]

            if len(batch) < request.page_size:
                break

            offset += request.page_size

        return collected

    def collect(
        self,
        path: str,
        *,
        page_size: int,
        extract_items: Extractor,
        params: Mapping[str, Any] | None = None,
        max_items: int | None = None,
    ) -> list[dict[str, Any]]:
        request = OffsetRequest(
            path=path,
            page_size=page_size,
            extract_items=extract_items,
            params=params,
            max_items=max_items,
        )
        return self.paginate(request)


@dataclass(slots=True)
class CursorPaginationStrategy:
    """Paginate APIs that use cursor continuation tokens."""

    client: UnifiedAPIClient
    cursor_param: str = "cursor"
    page_size_param: str = "per_page"

    def paginate(self, request: CursorRequest) -> list[dict[str, Any]]:
        """Collect paginated results using opaque cursor tokens."""

        if request.page_size <= 0:
            msg = "page_size must be a positive integer"
            raise ValueError(msg)

        collected: list[dict[str, Any]] = []
        seen: set[str] = set()
        cursor: str | None = None
        page_index = 0

        while True:
            if request.max_pages is not None and page_index >= request.max_pages:
                if request.on_page_limit is not None:
                    request.on_page_limit(page_index)
                break

            query: dict[str, Any] = dict(request.params or {})
            if self.page_size_param:
                query.setdefault(self.page_size_param, request.page_size)
            if cursor:
                query[self.cursor_param] = cursor

            payload = self.client.request_json(request.path, params=query)
            if not isinstance(payload, Mapping):
                if request.on_invalid_payload is not None:
                    request.on_invalid_payload(page_index)
                break

            page_index += 1

            batch: list[dict[str, Any]] = []
            for item in request.parse_items(payload):
                if not isinstance(item, Mapping):
                    continue
                key_value = item.get(request.unique_key)
                if key_value is None:
                    continue
                key_str = str(key_value).strip()
                if not key_str or key_str in seen:
                    continue
                seen.add(key_str)
                batch.append(dict(item))

            if not batch:
                if request.on_empty_page is not None:
                    request.on_empty_page(page_index)
                break

            collected.extend(batch)

            next_cursor = request.extract_cursor(payload)
            if not next_cursor:
                break

            cursor = next_cursor

        return collected

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
        request = CursorRequest(
            path=path,
            page_size=page_size,
            unique_key=unique_key,
            parse_items=parse_items,
            extract_cursor=extract_cursor,
            params=params,
            max_pages=max_pages,
            on_invalid_payload=on_invalid_payload,
            on_empty_page=on_empty_page,
            on_page_limit=on_page_limit,
        )
        return self.paginate(request)
@dataclass(slots=True)
class TokenInitialization(Generic[TState]):
    """Describe the initial state for token-based pagination."""

    state: TState
    total_count: int


@dataclass(slots=True)
class TokenPaginationStrategy(Generic[TState]):
    """Paginate APIs that require an initialization token exchange."""

    client: UnifiedAPIClient

    def paginate(self, request: TokenRequest[TState]) -> list[dict[str, Any]]:
        """Collect paginated records after exchanging continuation tokens."""

        if request.batch_size <= 0:
            msg = "batch_size must be a positive integer"
            raise ValueError(msg)

        init = request.initializer(self.client)
        if init is None or init.total_count <= 0:
            return []

        state = init.state
        total_count = init.total_count
        offset = 0
        collected: list[dict[str, Any]] = []

        while offset < total_count:
            chunk_size = min(request.batch_size, total_count - offset)
            items = request.fetcher(self.client, state, offset, chunk_size) or []
            batch = _coerce_batch(items)
            if not batch:
                break

            collected.extend(batch)
            offset += chunk_size

        return collected

    def collect(
        self,
        *,
        initializer: Callable[[UnifiedAPIClient], TokenInitialization[TState] | None],
        fetcher: Callable[[UnifiedAPIClient, TState, int, int], Sequence[Mapping[str, Any]] | None],
        batch_size: int,
    ) -> list[dict[str, Any]]:
        request: TokenRequest[TState] = TokenRequest(
            initializer=initializer,
            fetcher=fetcher,
            batch_size=batch_size,
        )
        return self.paginate(request)
