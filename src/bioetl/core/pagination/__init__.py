"""Reusable pagination strategies shared across external sources."""

from __future__ import annotations

from .strategy import (
    CursorPaginationStrategy,
    CursorRequest,
    OffsetPaginationStrategy,
    OffsetRequest,
    PageNumberPaginationStrategy,
    PageNumberRequest,
    TokenInitialization,
    TokenPaginationStrategy,
    TokenRequest,
)

__all__ = [
    "CursorPaginationStrategy",
    "CursorRequest",
    "OffsetPaginationStrategy",
    "OffsetRequest",
    "PageNumberPaginationStrategy",
    "PageNumberRequest",
    "TokenInitialization",
    "TokenPaginationStrategy",
    "TokenRequest",
]
