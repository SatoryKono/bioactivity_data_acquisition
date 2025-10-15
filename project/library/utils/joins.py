"""Utilities for joining source payloads."""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable


def merge_source_payloads(payloads: Iterable[dict[str, dict[str, object]]]) -> dict[str, dict[str, object]]:
    """Merge payloads grouped by source into combined dictionary.

    Input shape is ``[{"chembl": {...}}, {"pubmed": {...}}]`` and output merges per key.
    """
    merged: dict[str, dict[str, object]] = defaultdict(dict)
    for payload in payloads:
        for source, data in payload.items():
            merged[source].update(data)
    return dict(merged)


def flatten_payloads(payloads: dict[str, dict[str, object]]) -> dict[str, object]:
    """Flatten a mapping of source payloads into a single row dictionary."""
    flat: dict[str, object] = {}
    for source, data in payloads.items():
        for key, value in data.items():
            flat[f"{source}_{key}"] = value
    return flat


def merge_records(base: dict[str, object], records: list[dict[str, object]]) -> dict[str, object]:
    """Merge records with preference for first value and alternatives tracking."""
    result = base.copy()
    for record in records:
        for key, value in record.items():
            if key not in result:
                result[key] = value
            elif result[key] != value:
                alt_key = f"{key}__alternatives"
                if alt_key not in result:
                    result[alt_key] = [result[key]]
                alt_list = result.get(alt_key)
                # Ensure alt_list is a list before checking and appending
                if isinstance(alt_list, list) and value not in alt_list:
                    alt_list.append(value)
    return result

def ordered_sources(sources: Iterable[str]) -> list[str]:
    """Return sources in deterministic order for output columns."""
    priority = ["chembl", "pubmed", "semscholar", "crossref", "openalex"]
    seen = set()
    ordered: list[str] = []
    for src in priority:
        if src in sources:
            ordered.append(src)
            seen.add(src)
    for src in sources:
        if src not in seen:
            ordered.append(src)
    return ordered
