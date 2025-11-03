"""Text normalisation helpers for IUPHAR data."""

from __future__ import annotations

import re
from typing import Iterable

__all__ = [
    "normalize_target_name",
    "normalize_gene_symbol",
    "unique_preserving_order",
]

_NAME_CLEAN_RE = re.compile(r"[^a-z0-9]+")


def normalize_target_name(value: str | None) -> str:
    """Normalise a free-text target name for fuzzy matching."""

    if value is None:
        return ""
    normalized = str(value).lower()
    return _NAME_CLEAN_RE.sub("", normalized)


def normalize_gene_symbol(value: str | None) -> str:
    """Normalise gene symbols while retaining case sensitivity."""

    if value is None:
        return ""
    return str(value).strip()


def unique_preserving_order(values: Iterable[str]) -> list[str]:
    """Return unique values from ``values`` preserving their first occurrence."""

    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result
