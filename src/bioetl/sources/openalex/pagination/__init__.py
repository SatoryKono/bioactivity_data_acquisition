"""Cursor pagination helpers for the OpenAlex API."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.logger import UnifiedLogger

__all__ = ["CursorPaginator", "PageParser"]


PageParser = Callable[[Any], Sequence[Mapping[str, Any]]]

logger = UnifiedLogger.get(__name__)


@dataclass(slots=True)
class CursorPaginator:
    """Fetch OpenAlex collections using the ``cursor`` pagination strategy."""

    client: UnifiedAPIClient
    page_size: int = 100

    def fetch_all(
        self,
        path: str,
        *,
        unique_key: str = "id",
        params: Mapping[str, Any] | None = None,
        parser: PageParser | None = None,
        max_pages: int | None = None,
    ) -> list[dict[str, Any]]:
        """Iterate through all available pages for ``path``.

        Args:
            path: Relative API path (e.g. ``"/works"``).
            unique_key: Column used to deduplicate across pages.
            params: Additional query parameters (filters, fields, etc.).
            parser: Optional callable extracting result collections from the raw
                response. Defaults to reading ``response["results"]`` when
                omitted.
            max_pages: Optional safety limit used in tests to guard against
                misconfigured cursors.

        Returns:
            A list of dictionaries representing the aggregated results.
        """

        collected: list[dict[str, Any]] = []
        seen: set[str] = set()
        cursor: str | None = None
        page_count = 0

        while True:
            if max_pages is not None and page_count >= max_pages:
                logger.warning("openalex_cursor_page_limit", path=path, pages=max_pages)
                break

            query: dict[str, Any] = dict(params or {})
            query.setdefault("per_page", self.page_size)
            if cursor:
                query["cursor"] = cursor

            payload = self.client.request_json(path, params=query)
            if not isinstance(payload, Mapping):
                logger.warning("openalex_cursor_invalid_payload", path=path)
                break

            page_count += 1

            items: Sequence[Mapping[str, Any]]
            if parser is None:
                raw_items = payload.get("results", [])
                if not isinstance(raw_items, Sequence):
                    raw_items = []
                items = [item for item in raw_items if isinstance(item, Mapping)]
            else:
                items = list(parser(payload))

            batch: list[dict[str, Any]] = []
            for item in items:
                key_value = item.get(unique_key)
                if key_value is None:
                    continue

                key_str = str(key_value).strip()
                if not key_str or key_str in seen:
                    continue

                seen.add(key_str)
                batch.append(dict(item))

            if not batch:
                logger.info("openalex_cursor_no_results", path=path, page=page_count)
                break

            collected.extend(batch)

            meta = payload.get("meta")
            next_cursor = meta.get("next_cursor") if isinstance(meta, Mapping) else None
            if not next_cursor:
                break

            cursor = str(next_cursor)

        return collected
