"""Normalisation helpers for publication identifiers and text fields."""
from __future__ import annotations

import re
from typing import Optional


_DOI_PREFIX = re.compile(r"^https?://(dx\.)?doi\.org/", re.IGNORECASE)
_WHITESPACE = re.compile(r"\s+")


def coerce_text(value: object) -> Optional[str]:
    """Convert arbitrary values to trimmed strings, returning ``None`` for empty values."""

    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()
    return text or None


def to_lc_stripped(value: object) -> Optional[str]:
    text = coerce_text(value)
    return text.lower() if text is not None else None


def normalise_doi(value: object) -> Optional[str]:
    """Normalise a DOI by stripping URL prefixes, trimming whitespace and lower-casing."""

    text = coerce_text(value)
    if text is None:
        return None
    text = _DOI_PREFIX.sub("", text)
    text = _WHITESPACE.sub("", text)
    text = text.strip().strip(".")
    if not text:
        return None
    return text.lower()

