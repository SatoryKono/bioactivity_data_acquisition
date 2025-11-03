"""Shared helper utilities for normalizers."""

from __future__ import annotations

import math
from typing import Any

from bioetl.normalizers.constants import BOOLEAN_FALSE, BOOLEAN_TRUE, NA_STRINGS


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


def coerce_bool(
    value: Any, *, default: bool | None = None, allow_na: bool = True
) -> bool | None:
    """Coerce *value* to a boolean according to shared normalisation rules.

    The conversion follows the historical project semantics that are duplicated in
    :class:`bioetl.normalizers.numeric.NumericNormalizer` and
    :class:`bioetl.normalizers.numeric.BooleanNormalizer`:

    * Missing values detected via :func:`_is_na` return ``None`` when
      ``allow_na`` is ``True`` and fall back to *default* otherwise.
    * Existing boolean instances are returned as-is.
    * Integer and float values are cast using :class:`bool`.
    * String values are stripped and compared case-insensitively against
      :data:`bioetl.normalizers.constants.BOOLEAN_TRUE` and
      :data:`bioetl.normalizers.constants.BOOLEAN_FALSE`.
    * Unrecognised values fall back to *default*.
    """

    if _is_na(value):
        if allow_na:
            return None
        return default

    if isinstance(value, bool):
        return value

    if isinstance(value, int) and not isinstance(value, bool):
        return bool(value)

    if isinstance(value, float):
        return bool(value)

    text = str(value).strip().lower()
    if text in BOOLEAN_TRUE:
        return True
    if text in BOOLEAN_FALSE:
        return False

    return default


__all__ = ["_is_na", "is_na", "coerce_bool"]
