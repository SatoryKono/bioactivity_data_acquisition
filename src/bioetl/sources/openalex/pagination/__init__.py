"""Cursor pagination helpers for the OpenAlex API."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger
from bioetl.core.pagination import CursorPaginationStrategy

__all__ = ["CursorPaginator", "PageParser"]


PageParser = Callable[[Any], Sequence[Mapping[str, Any]]]

logger = UnifiedLogger.get(__name__)


@dataclass(slots=True)
class CursorPaginator:
    """Fetch OpenAlex collections using the ``cursor`` pagination strategy."""

    client: UnifiedAPIClient
    page_size: int = 100
    _strategy: CursorPaginationStrategy = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_strategy", CursorPaginationStrategy(self.client))

    def fetch_all(
        self,
        path: str,
        *,
        unique_key: str = "id",
        params: Mapping[str, Any] | None = None,
        parser: PageParser | None = None,
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        """Iterate through all available pages for ``path``."""

        def parse_items(payload: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
            if parser is None:
                raw_items = payload.get("results", [])
                if not isinstance(raw_items, Sequence):
                    return []
                return [item for item in raw_items if isinstance(item, Mapping)]
            return list(parser(payload))

        def extract_cursor(payload: Mapping[str, Any]) -> str | None:
            meta = payload.get("meta")
            if not isinstance(meta, Mapping):
                return None
            next_cursor = meta.get("next_cursor")
            if isinstance(next_cursor, str) and next_cursor:
                return next_cursor
            return None

        def handle_invalid_payload(_: int) -> None:
            logger.warning("openalex_cursor_invalid_payload", path=path)

        def handle_empty_page(page_index: int) -> None:
            logger.info("openalex_cursor_no_results", path=path, page=page_index)

        def handle_page_limit(page_index: int) -> None:
            logger.warning("openalex_cursor_page_limit", path=path, pages=page_index)

        return self._strategy.collect(
            path,
            page_size=self.page_size,
            unique_key=unique_key,
            parse_items=parse_items,
            extract_cursor=extract_cursor,
            params=params,
            max_pages=max_pages,
            on_invalid_payload=handle_invalid_payload,
            on_empty_page=handle_empty_page,
            on_page_limit=handle_page_limit,
        )
