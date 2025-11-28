"""Compatibility mixins for legacy ChEMBL client methods."""
from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Any
import warnings

from bioetl.core.http.pagination_helpers import (
    DEFAULT_NEXT_KEY,
    DEFAULT_PAGE_KEY,
    DEFAULT_PAGE_PARAM,
)


class ChemblCompatibilityMixin:
    """Provide deprecated aliases for ChEMBL client pagination methods."""

    def fetch_page(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[dict[str, Any]]:
        """Deprecated alias for ``fetch_many``."""

        warnings.warn(
            "fetch_page is deprecated; use fetch_many instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch_many(  # type: ignore[attr-defined]
            page_size=page_size,
            params=params,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )

    def list(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[dict[str, Any]]:
        """Deprecated alias for ``fetch_many``."""

        warnings.warn(
            "list is deprecated; use fetch_many instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch_many(  # type: ignore[attr-defined]
            page_size=page_size,
            params=params,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )

    def fetch_all(
        self,
        *,
        page_size: int = 1000,
        params: Mapping[str, Any] | None = None,
        page_key: str = DEFAULT_PAGE_KEY,
        next_key: str = DEFAULT_NEXT_KEY,
        page_param: str | None = DEFAULT_PAGE_PARAM,
    ) -> Iterator[dict[str, Any]]:
        """Deprecated alias for enumerating all paginated entities."""

        warnings.warn(
            "fetch_all is deprecated; use fetch_many instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fetch_many(  # type: ignore[attr-defined]
            page_size=page_size,
            params=params,
            page_key=page_key,
            next_key=next_key,
            page_param=page_param,
        )


__all__ = ["ChemblCompatibilityMixin"]
