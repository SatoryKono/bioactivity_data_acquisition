"""Common runtime type-checking helpers used across the project."""

from __future__ import annotations

from typing import Any, TypeGuard

__all__ = ["is_list", "is_dict"]


def is_list(value: Any) -> TypeGuard[list[Any]]:
    """Return ``True`` when ``value`` is a ``list`` instance."""

    return isinstance(value, list)


def is_dict(value: Any) -> TypeGuard[dict[str, Any]]:
    """Return ``True`` when ``value`` is a ``dict`` instance."""

    return isinstance(value, dict)
