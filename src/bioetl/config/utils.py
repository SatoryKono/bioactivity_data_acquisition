"""Configuration utility functions."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .helpers import coerce_bool


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
