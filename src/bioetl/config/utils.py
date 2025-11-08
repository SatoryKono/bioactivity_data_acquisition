"""Configuration utility functions."""

from __future__ import annotations

from typing import Any

from pydantic import SecretStr

_TRUE_LITERALS: frozenset[str] = frozenset({"1", "true", "yes", "on"})
_FALSE_LITERALS: frozenset[str] = frozenset({"0", "false", "no", "off"})


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
    if isinstance(value, SecretStr):
        secret_value = value.get_secret_value()
        if secret_value is None:
            return False
        return coerce_bool(secret_value)
    if value is None:
        return False
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _TRUE_LITERALS:
            return True
        if normalized in _FALSE_LITERALS:
            return False
    return bool(value)
