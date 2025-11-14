"""Shared constants and helpers reused across schema modules."""

from __future__ import annotations

from collections.abc import Sequence

HASH_COLUMN_NAMES: tuple[str, str] = ("hash_row", "hash_business_key")
RELATION_SYMBOLS: tuple[str, ...] = ("=", "<", ">", "~")
HTTP_URL_PATTERN: str = r"^https?://"


def resolve_row_hash_fields(column_order: Sequence[str]) -> tuple[str, ...]:
    """Return columns contributing to row-level hashes in deterministic order."""

    return tuple(column for column in column_order if column not in HASH_COLUMN_NAMES)


__all__ = [
    "HASH_COLUMN_NAMES",
    "RELATION_SYMBOLS",
    "HTTP_URL_PATTERN",
    "resolve_row_hash_fields",
]

