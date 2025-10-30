"""Helpers for normalizing bibliographic metadata."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any

from bioetl.normalizers.registry import registry

_DOI_URL_PATTERN = re.compile(r"^(?:https?://)?(?:dx\.)?doi\.org/", flags=re.IGNORECASE)
_DOI_LABEL_PATTERN = re.compile(r"^doi[:\s]+", flags=re.IGNORECASE)


def _first_non_empty_string(value: Any) -> str | None:
    """Extract the first non-empty string from value."""

    if isinstance(value, str):
        candidate = value.strip()
        return candidate or None

    if isinstance(value, Mapping):
        return None

    if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray)):
        for item in value:
            candidate = _first_non_empty_string(item)
            if candidate:
                return candidate

    return None


def normalize_doi(value: Any) -> str | None:
    """Normalize DOI value using the identifier normalizer registry entry."""

    candidate = _first_non_empty_string(value)
    if not candidate:
        return None

    candidate = _DOI_URL_PATTERN.sub("", candidate)
    candidate = _DOI_LABEL_PATTERN.sub("", candidate).strip()
    if not candidate:
        return None

    return registry.normalize("identifier", candidate)


def normalize_title(value: Any) -> str | None:
    """Normalize bibliographic title values."""

    candidate = _first_non_empty_string(value)
    if not candidate:
        return None

    return registry.normalize("string", candidate)


def _normalize_author_mapping(author: Mapping[str, Any]) -> str | None:
    """Normalize a single author mapping to a display string."""

    nested = author.get("author")
    if isinstance(nested, Mapping):
        return _normalize_author_mapping(nested)

    for key in ("display_name", "displayName", "name"):
        value = author.get(key)
        if isinstance(value, str):
            return registry.normalize("string", value)

    family = author.get("family") or author.get("last") or author.get("last_name") or author.get("lastName")
    given = author.get("given") or author.get("first") or author.get("first_name") or author.get("firstName")

    parts = []
    if family:
        parts.append(str(family))
    if given:
        if parts:
            parts.append(str(given))
        else:
            parts.append(str(given))

    if not parts:
        return None

    if len(parts) == 2:
        name = f"{parts[0]}, {parts[1]}"
    else:
        name = parts[0]

    return registry.normalize("string", name)


def normalize_authors(value: Any) -> str | None:
    """Normalize authors information to a semicolon-delimited string."""

    if value is None:
        return None

    if isinstance(value, str):
        return registry.normalize("string", value)

    entries: Iterable[Any]
    if isinstance(value, Mapping):
        entries = [value]
    elif isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, str)):
        entries = value
    else:
        return None

    names: list[str] = []
    for entry in entries:
        normalized_name: str | None = None

        if isinstance(entry, str):
            normalized_name = registry.normalize("string", entry)
        elif isinstance(entry, Mapping):
            normalized_name = _normalize_author_mapping(entry)

        if normalized_name:
            names.append(normalized_name)

    if not names:
        return None

    joined = "; ".join(names)
    return registry.normalize("string", joined)

