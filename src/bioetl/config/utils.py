"""Configuration utility functions."""

from __future__ import annotations

from typing import Any


def coerce_bool(value: Any) -> bool:
    """Best-effort conversion for boolean-like configuration values.

    Parameters
    ----------
    value
        Value to coerce to boolean.

    Returns
    -------
    bool
        Coerced boolean value.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return bool(value)
