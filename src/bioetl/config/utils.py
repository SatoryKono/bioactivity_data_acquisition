"""Configuration utility functions."""

from __future__ import annotations

from collections.abc import Mapping
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


def coerce_max_url_length(parameters: Mapping[str, Any] | None) -> int:
    """Normalize ``max_url_length`` from raw configuration parameters."""

    mapping = parameters or {}
    raw = mapping.get("max_url_length")
    if raw is None:
        return 2000
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        msg = "max_url_length must be coercible to an integer"
        raise ValueError(msg) from exc
    if value <= 0:
        msg = "max_url_length must be a positive integer"
        raise ValueError(msg)
    return value
