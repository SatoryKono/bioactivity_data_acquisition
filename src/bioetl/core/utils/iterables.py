"""Iterable helper utilities."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, TypeGuard

__all__ = ["is_non_string_iterable"]


def is_non_string_iterable(value: Any) -> TypeGuard[Iterable[Any]]:
    """Return True for iterables that are not string-like."""

    return isinstance(value, Iterable) and not isinstance(value, (str, bytes, bytearray))
