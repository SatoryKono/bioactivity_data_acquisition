"""Helpers for parsing UniProt entry responses."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any


def _coerce_entry(entry: Any) -> tuple[str, dict[str, Any]] | None:
    """Return the canonical accession and entry payload if valid."""

    if not isinstance(entry, Mapping):
        return None

    primary = entry.get("primaryAccession") or entry.get("accession")
    if not primary:
        return None

    return str(primary), dict(entry)


def parse_search_response(payload: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Parse a UniProt search response into a mapping of accession to entry."""

    if not isinstance(payload, Mapping):
        return {}

    raw_results = payload.get("results") or payload.get("entries") or []
    if isinstance(raw_results, Mapping):
        raw_results = [raw_results]

    entries: dict[str, dict[str, Any]] = {}
    if not isinstance(raw_results, Sequence):
        return entries

    for item in raw_results:
        coerced = _coerce_entry(item)
        if coerced is None:
            continue
        accession, entry = coerced
        entries[accession] = entry
    return entries


def parse_entry_response(payload: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Parse a UniProt entry response returning the same structure as searches."""

    if not isinstance(payload, Mapping):
        return {}

    # Some endpoints return the entry directly, others wrap it under "entry".
    entry_candidate: Iterable[Any]
    if isinstance(payload.get("entry"), Mapping):
        entry_candidate = [payload["entry"]]
    else:
        entry_candidate = [payload]

    entries: dict[str, dict[str, Any]] = {}
    for item in entry_candidate:
        coerced = _coerce_entry(item)
        if coerced is None:
            continue
        accession, entry = coerced
        entries[accession] = entry
    return entries
