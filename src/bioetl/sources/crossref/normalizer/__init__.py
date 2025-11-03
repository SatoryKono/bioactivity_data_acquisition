"""Helpers for normalising Crossref specific payload sections."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from bioetl.adapters._normalizer_helpers import get_bibliography_normalizers
from bioetl.normalizers.bibliography import normalize_authors as _normalize_authors

__all__ = [
    "normalize_crossref_authors",
    "normalize_crossref_affiliations",
]

_, _STRING_NORMALIZER = get_bibliography_normalizers()


def normalize_crossref_authors(value: Any) -> str | None:
    """Normalise Crossref author payloads into a deterministic string."""

    return _normalize_authors(value)


def normalize_crossref_affiliations(value: Any) -> str | None:
    """Normalise author affiliation blocks returned by Crossref."""

    entries: Iterable[Any]
    if value is None:
        return None
    if isinstance(value, Mapping):
        entries = [value]
    elif isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, str)):
        entries = value
    else:
        return None

    seen: set[str] = set()
    affiliations: list[str] = []

    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        raw_affiliations = entry.get("affiliation")
        if isinstance(raw_affiliations, Mapping):
            raw_affiliations = [raw_affiliations]
        if not isinstance(raw_affiliations, Iterable):
            continue

        for affiliation in raw_affiliations:
            if not isinstance(affiliation, Mapping):
                continue
            name = affiliation.get("name") or affiliation.get("Name")
            if not isinstance(name, str):
                continue
            normalised = _STRING_NORMALIZER.normalize(name)
            if not normalised:
                continue
            key = normalised.lower()
            if key in seen:
                continue
            seen.add(key)
            affiliations.append(normalised)

    if not affiliations:
        return None

    joined = "; ".join(affiliations)
    return _STRING_NORMALIZER.normalize(joined)
