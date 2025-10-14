"""Normalisation helpers."""
from __future__ import annotations

import re
from typing import Optional

DOI_PATTERN = re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.IGNORECASE)


def normalise_doi(doi: str | None) -> Optional[str]:
    """Normalise DOI strings to lower-case canonical form."""
    if not doi:
        return None
    match = DOI_PATTERN.search(doi.strip())
    if not match:
        return None
    return match.group(0).lower()


def to_lc_stripped(value: str | None) -> Optional[str]:
    """Lower-case and strip whitespace."""
    if value is None:
        return None
    return value.strip().lower() or None


def coerce_text(value: object) -> Optional[str]:
    """Convert arbitrary values to stripped text."""
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return str(value).strip() or None
