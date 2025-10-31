"""Pagination helpers for the Semantic Scholar API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from bioetl.core.api_client import UnifiedAPIClient

__all__ = ["OffsetPaginator"]


@dataclass(slots=True)
class OffsetPaginator:
    """Offset-based paginator for Semantic Scholar search endpoint."""

    client: UnifiedAPIClient
    page_size: int = 100

    def fetch_all(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        max_items: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch and aggregate all pages from the search endpoint."""

        collected: list[dict[str, Any]] = []
        offset = 0

        while True:
            query: dict[str, Any] = dict(params or {})
            query["limit"] = self.page_size
            query["offset"] = offset

            payload = self.client.request_json(path, params=query)
            if not isinstance(payload, Mapping):
                break

            items = payload.get("data", [])
            if not isinstance(items, list) or not items:
                break

            for item in items:
                if isinstance(item, Mapping):
                    collected.append(dict(item))

            if max_items is not None and len(collected) >= max_items:
                return collected[:max_items]

            if len(items) < self.page_size:
                break

            offset += self.page_size

        return collected
