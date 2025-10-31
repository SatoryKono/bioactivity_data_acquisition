"""Pagination helpers for the Semantic Scholar API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.pagination import OffsetPaginationStrategy

__all__ = ["OffsetPaginator"]


@dataclass(slots=True)
class OffsetPaginator:
    """Offset-based paginator for Semantic Scholar search endpoint."""

    client: UnifiedAPIClient
    page_size: int = 100

    def __post_init__(self) -> None:
        self._strategy = OffsetPaginationStrategy(self.client)

    def fetch_all(
        self,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        max_items: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch and aggregate all pages from the search endpoint."""

        def extract_items(payload: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
            items = payload.get("data", [])
            if isinstance(items, Sequence):
                return [item for item in items if isinstance(item, Mapping)]
            return []

        return self._strategy.collect(
            path,
            page_size=self.page_size,
            params=params,
            max_items=max_items,
            extract_items=extract_items,
        )
