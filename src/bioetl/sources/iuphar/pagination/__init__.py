"""Pagination helpers for the IUPHAR API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from bioetl.core.api_client import UnifiedAPIClient
from bioetl.core.pagination import PageNumberPaginationStrategy, PageNumberRequest

__all__ = ["PageNumberPaginator"]

PageParser = Callable[[Any], Sequence[Mapping[str, Any]]]


@dataclass(slots=True)
class PageNumberPaginator:
    """Simple page-number paginator with duplicate filtering."""

    client: UnifiedAPIClient
    page_size: int

    def __post_init__(self) -> None:
        self._strategy = PageNumberPaginationStrategy(self.client)

    def fetch_all(
        self,
        path: str,
        *,
        unique_key: str,
        params: Mapping[str, Any] | None = None,
        parser: PageParser | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch all pages for ``path`` until an empty page is returned."""

        if parser is None:
            msg = "parser must be provided for IUPHAR pagination"
            raise ValueError(msg)

        request = PageNumberRequest(
            path=path,
            page_size=self.page_size,
            unique_key=unique_key,
            parser=parser,
            params=params,
        )
        return self._strategy.paginate(request)
