"""Pagination helpers for the Crossref REST API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from bioetl.core.api_client import UnifiedAPIClient

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

    def fetch_all(
        self,
        path: str,
        *,
        unique_key: str = "DOI",
        params: Mapping[str, Any] | None = None,
        parser: PageParser | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch every page for ``path`` until the cursor is exhausted.

        Args:
            path: API endpoint path (``/works`` or ``/journals/{issn}/works``).
            unique_key: Field used to remove duplicates between pages.
            params: Base query parameters (filters, facets, etc.).
            parser: Optional callable extracting items from the response body.

        Returns:
            List of unique payload dictionaries in the order they were
            retrieved.
        """

        collected: list[dict[str, Any]] = []
        seen: set[str] = set()
        cursor: str | None = None

        while True:
            query: dict[str, Any] = dict(params or {})
            query["rows"] = self.page_size
            if cursor:
                query["cursor"] = cursor

            payload = self.client.request_json(path, params=query)
            if not isinstance(payload, Mapping):
                break

            message = payload.get("message")
            if not isinstance(message, Mapping):
                break

            if parser is None:
                items = message.get("items", [])
            else:
                items = list(parser(message))

            filtered: list[dict[str, Any]] = []
            for item in items:
                if not isinstance(item, Mapping):
                    continue

                key_value = item.get(unique_key)
                if key_value is None:
                    continue

                key_str = str(key_value).strip()
                if not key_str or key_str in seen:
                    continue

                seen.add(key_str)
                filtered.append(dict(item))

            if not filtered:
                break

            collected.extend(filtered)

            next_cursor = message.get("next-cursor")
            if not isinstance(next_cursor, str) or not next_cursor.strip():
                break

            cursor = next_cursor

        return collected
