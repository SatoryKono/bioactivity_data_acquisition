"""Utilities for working with Pandera schema metadata payloads."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

__all__ = ["normalize_sequence", "metadata_dict"]


def normalize_sequence(value: Any) -> tuple[str, ...]:
    """Return a tuple of string values from ``value``.

    The helper accepts sequences, iterables and scalar string values.
    ``None`` and unsupported inputs are normalised to an empty tuple. Bytes
    payloads are cast to their ``str`` representation for backwards
    compatibility with existing schema helpers.
    """

    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, bytes):
        return (str(value),)
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        return tuple(str(item) for item in value)
    return ()


def metadata_dict(*sources: Mapping[str, Any] | None, **updates: Any) -> dict[str, Any]:
    """Merge ``sources`` into a shallow copy suitable for metadata operations."""

    merged: dict[str, Any] = {}
    for source in sources:
        if source:
            merged.update(dict(source))
    if updates:
        merged.update(updates)
    return merged
