"""Parsing helpers for UniProt ID mapping responses."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any


def parse_idmapping_status(payload: Mapping[str, Any] | None) -> str:
    """Return a normalized job status from a UniProt ID mapping status response."""

    if not isinstance(payload, Mapping):
        return "unknown"

    status = payload.get("jobStatus") or payload.get("status")
    if not isinstance(status, str):
        return "unknown"

    normalized = status.strip().lower()
    if normalized in {"pending", "queued"}:
        return "pending"
    if normalized in {"running", "active"}:
        return "running"
    if normalized in {"finished", "success", "completed", "complete"}:
        return "finished"
    if normalized in {"failed", "error", "cancelled", "canceled"}:
        return "failed"
    return normalized or "unknown"


def parse_idmapping_results(payload: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    """Parse UniProt ID mapping results into a normalized list of records."""

    if not isinstance(payload, Mapping):
        return []

    raw_results: Iterable[Any] = payload.get("results") or payload.get("mappedTo") or []
    if isinstance(raw_results, Mapping):
        raw_results = [raw_results]

    if not isinstance(raw_results, Sequence):
        return []

    records: list[dict[str, Any]] = []
    for item in raw_results:
        if not isinstance(item, Mapping):
            continue
        submitted = item.get("from") or item.get("accession") or item.get("input")
        to_entry = item.get("to") or item.get("mappedTo") or {}
        canonical: str | None
        isoform: str | None
        if isinstance(to_entry, Mapping):
            canonical_value = to_entry.get("primaryAccession") or to_entry.get("accession")
            isoform_value = to_entry.get("isoformAccession") or to_entry.get("isoform")
            canonical = str(canonical_value) if canonical_value else None
            isoform = str(isoform_value) if isoform_value else None
        else:
            canonical = str(to_entry) if to_entry else None
            isoform = None

        record = {
            "submitted_id": str(submitted) if submitted else None,
            "canonical_accession": canonical,
            "isoform_accession": isoform,
        }
        records.append(record)

    return records
