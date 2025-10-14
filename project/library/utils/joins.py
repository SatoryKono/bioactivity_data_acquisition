"""Utilities for joining source payloads."""
from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, List


def merge_source_payloads(payloads: Iterable[Dict[str, Dict[str, object]]]) -> Dict[str, Dict[str, object]]:
    """Merge payloads grouped by source into combined dictionary.

    Input shape is ``[{"chembl": {...}}, {"pubmed": {...}}]`` and output merges per key.
    """
    merged: Dict[str, Dict[str, object]] = defaultdict(dict)
    for payload in payloads:
        for source, data in payload.items():
            merged[source].update(data)
    return dict(merged)


def flatten_payloads(payloads: Dict[str, Dict[str, object]]) -> Dict[str, object]:
    """Flatten a mapping of source payloads into a single row dictionary."""
    flat: Dict[str, object] = {}
    for source, data in payloads.items():
        for key, value in data.items():
            flat[f"{source}_{key}"] = value
    return flat


def ordered_sources(sources: Iterable[str]) -> List[str]:
    """Return sources in deterministic order for output columns."""
    priority = ["chembl", "pubmed", "semscholar", "crossref", "openalex"]
    seen = set()
    ordered: List[str] = []
    for src in priority:
        if src in sources:
            ordered.append(src)
            seen.add(src)
    for src in sources:
        if src not in seen:
            ordered.append(src)
    return ordered
