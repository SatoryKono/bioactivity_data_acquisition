"""Reusable mixins and helpers shared across HTTP clients."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

__all__ = [
    "normalize_select_fields",
    "merge_select_fields",
    "build_filters_payload",
]


def normalize_select_fields(
    value: object,
    *,
    default: Sequence[str] | None = None,
) -> tuple[str, ...] | None:
    """Normalize select_fields input into an immutable tuple."""

    if value is None:
        return tuple(default) if default else None

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        normalized: list[str] = []
        for raw in value:
            if raw is None:
                continue
            text = str(raw).strip()
            if text and text not in normalized:
                normalized.append(text)
        if normalized:
            return tuple(normalized)
        return tuple(default) if default else None

    text_value = str(value).strip()
    if not text_value:
        return tuple(default) if default else None
    if default and text_value in default and len(default) == 1:
        return tuple(default)
    return (text_value,)


def merge_select_fields(
    select_fields: Sequence[str] | None,
    required_fields: Sequence[str] | None = None,
) -> tuple[str, ...] | None:
    """Merge configured select_fields with required fields deterministically."""

    if not select_fields and not required_fields:
        return None

    merged: list[str] = []
    for candidate in select_fields or ():
        value = candidate.strip()
        if value and value not in merged:
            merged.append(value)
    for candidate in required_fields or ():
        value = candidate.strip()
        if value and value not in merged:
            merged.append(value)
    return tuple(merged)


def build_filters_payload(
    *,
    limit: int | None,
    page_size: int | None,
    select_fields: Sequence[str] | None,
    extra_filters: Mapping[str, Any] | None = None,
    parameters: Mapping[str, Any] | None = None,
    mode: str = "all",
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Construct deterministic filter payloads and compact representation."""

    payload: dict[str, Any] = {
        "mode": mode,
        "limit": int(limit) if limit is not None else None,
        "page_size": page_size,
        "select_fields": list(select_fields) if select_fields else None,
    }
    if extra_filters:
        for key in sorted(extra_filters):
            payload[key] = extra_filters[key]
    if parameters:
        payload["parameters"] = dict(parameters)

    compact = {key: value for key, value in payload.items() if value is not None}
    return payload, compact

