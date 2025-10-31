"""Pagination helpers for the Crossref REST API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.pagination import CursorPaginationStrategy, CursorRequest

__all__ = ["CursorPaginator"]

PageParser = Callable[[Any], Sequence[Mapping[str, Any]]]


@dataclass(slots=True)
class CursorPaginator:
    """Cursor-based paginator tailored for the Crossref ``/works`` endpoint.

    The Crossref REST API supports cursor-based pagination when requesting large
    result sets.  Clients provide an opaque ``cursor`` token via the query
    string and the response embeds the next continuation token under the
    ``message.next-cursor`` field.  This helper centralises the iteration logic
    while deduplicating records based on a configurable business key.
    """

    client: UnifiedAPIClient
    page_size: int = 100

    def __post_init__(self) -> None:
        self._strategy = CursorPaginationStrategy(self.client, page_size_param="rows")

    def fetch_all(
        self,
        path: str,
        *,
        unique_key: str = "DOI",
        params: Mapping[str, Any] | None = None,
        parser: PageParser | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch every page for ``path`` until the cursor is exhausted."""

        def parse_items(payload: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
            message = payload.get("message")
            if not isinstance(message, Mapping):
                return []
            if parser is None:
                raw_items = message.get("items", [])
                if not isinstance(raw_items, Sequence):
                    return []
                return [item for item in raw_items if isinstance(item, Mapping)]
            return list(parser(message))

        def extract_cursor(payload: Mapping[str, Any]) -> str | None:
            message = payload.get("message")
            if not isinstance(message, Mapping):
                return None
            next_cursor = message.get("next-cursor")
            if isinstance(next_cursor, str) and next_cursor.strip():
                return next_cursor
            return None

        request = CursorRequest(
            path=path,
            page_size=self.page_size,
            unique_key=unique_key,
            parse_items=parse_items,
            extract_cursor=extract_cursor,
            params=params,
        )
        return self._strategy.paginate(request)
