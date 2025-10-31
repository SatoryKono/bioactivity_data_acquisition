"""Response parsing utilities for the IUPHAR API."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

__all__ = ["coerce_items", "parse_api_response"]


def coerce_items(payload: Any, unique_key: str) -> list[Mapping[str, Any]]:
    """Normalise API payloads into a list of mappings."""

    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, Mapping)]
    if isinstance(payload, Mapping):
        for key in ("results", "data", "items", "records"):
            items = payload.get(key)
            if isinstance(items, list):
                return [item for item in items if isinstance(item, Mapping)]
        if unique_key in payload:
            return [payload]
        nested = [value for value in payload.values() if isinstance(value, Mapping)]
        if nested:
            return [value for value in nested if isinstance(value, Mapping)]
    return []


def parse_api_response(payload: Any, *, unique_key: str) -> Sequence[Mapping[str, Any]]:
    """Parse a raw API payload into iterable items."""

    return coerce_items(payload, unique_key)
