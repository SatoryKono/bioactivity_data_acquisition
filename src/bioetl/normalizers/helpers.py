"""Shared helper utilities for normalizers."""

from __future__ import annotations

import math
from typing import Any

from bioetl.normalizers.constants import NA_STRINGS


def _is_na(value: Any) -> bool:
    """Return ``True`` when *value* should be treated as a missing entry.

    The helper normalises the common NA semantics across the project:

    * ``None`` is considered missing.
    * ``float`` values equal to ``NaN`` (``math.isnan``) are missing.
    * Strings are stripped of surrounding whitespace.  Empty strings and
      case-insensitive matches of :data:`bioetl.normalizers.constants.NA_STRINGS`
      are missing.

    Other data types – including integers, booleans, containers and custom
    objects – are treated as present and return ``False``.
    """

    if value is None:
        return True

    if isinstance(value, float) and math.isnan(value):
        return True

    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return True
        return stripped.lower() in NA_STRINGS

    return False


def is_na(value: Any) -> bool:
    """Public wrapper mirroring :func:`_is_na` for explicit imports."""

    return _is_na(value)


__all__ = ["_is_na", "is_na"]

