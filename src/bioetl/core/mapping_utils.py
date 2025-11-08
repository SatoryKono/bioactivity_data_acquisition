"""Utility helpers for working with mapping-like inputs."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def stringify_mapping(mapping: Mapping[Any, Any]) -> dict[str, Any]:
    """Return a copy of ``mapping`` with stringified keys."""

    return {str(key): value for key, value in mapping.items()}


__all__ = ["stringify_mapping"]
