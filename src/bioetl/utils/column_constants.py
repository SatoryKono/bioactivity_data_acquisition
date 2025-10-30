"""Constants and utilities for column validation."""

from __future__ import annotations

from collections.abc import Iterable

DEFAULT_COLUMN_VALIDATION_IGNORE_SUFFIXES: tuple[str, ...] = (
    "_quality_report.csv",
    "_correlation_report.csv",
    "_qc.csv",
    "_metadata.csv",
    "_qc_summary.json",
    "qc_missing_mappings.csv",
    "qc_enrichment_metrics.csv",
    "_summary_statistics.csv",
    "_dataset_metrics.csv",
)


def normalise_ignore_suffixes(values: Iterable[str]) -> tuple[str, ...]:
    """Normalise suffix values by trimming, lower-casing and removing duplicates."""

    normalised: list[str] = []
    for value in values:
        candidate = str(value).strip().lower()
        if not candidate:
            continue
        if candidate not in normalised:
            normalised.append(candidate)
    return tuple(normalised)


__all__ = [
    "DEFAULT_COLUMN_VALIDATION_IGNORE_SUFFIXES",
    "normalise_ignore_suffixes",
]

