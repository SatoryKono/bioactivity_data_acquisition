"""Pagination helpers for the IUPHAR API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from bioetl.core.api_client import UnifiedAPIClient

__all__ = ["PageNumberPaginator"]

PageParser = Callable[[Any], Sequence[Mapping[str, Any]]]


@dataclass(slots=True)
class PageNumberPaginator:
    """Simple page-number paginator with duplicate filtering."""

    client: UnifiedAPIClient
    page_size: int

    def fetch_all(
        self,
        path: str,
        *,
        unique_key: str,
        params: Mapping[str, Any] | None = None,
        parser: PageParser | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all pages for ``path`` until an empty page is returned."""

        collected: list[dict[str, Any]] = []
        seen: set[Any] = set()
        page = 1

        while True:
            query: dict[str, Any] = dict(params or {})
            query.setdefault("page", page)
            query.setdefault("pageSize", self.page_size)
            query.setdefault("offset", (page - 1) * self.page_size)
            query.setdefault("limit", self.page_size)

            if parser is None:
                raise ValueError("parser must be provided for IUPHAR pagination")

            payload = self.client.request_json(path, params=query)
            items = list(parser(payload))

            filtered: list[dict[str, Any]] = []
            for item in items:
                if not isinstance(item, Mapping):
                    continue
                key_value = item.get(unique_key)
                if key_value is not None and key_value in seen:
                    continue
                if key_value is not None:
                    seen.add(key_value)
                filtered.append(dict(item))

            if not filtered:
                break

            collected.extend(filtered)

            if len(filtered) < self.page_size:
                break

            page += 1

        return collected
