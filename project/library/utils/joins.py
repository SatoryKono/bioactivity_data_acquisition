"""Utilities for merging client payloads into a unified record."""
from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping


def merge_records(base: Mapping[str, Any], payloads: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    """Merge payloads preferring the first non-empty value for every field."""

    result: Dict[str, Any] = dict(base)
    for payload in payloads:
        for key, value in payload.items():
            if value in (None, ""):
                continue
            if key not in result or result[key] in (None, ""):
                result[key] = value
            else:
                # Keep values deterministic: convert conflicting scalars to first value, but append
                # alternative values to a list for traceability.
                if result[key] == value:
                    continue
                list_key = f"{key}__alternatives"
                existing = result.get(list_key)
                if isinstance(existing, list):
                    if value not in existing:
                        existing.append(value)
                else:
                    alt_list = [result[key]]
                    if value not in alt_list:
                        alt_list.append(value)
                    result[list_key] = alt_list
    return result

