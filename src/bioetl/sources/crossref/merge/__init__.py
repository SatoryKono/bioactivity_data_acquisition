"""Merge helpers for Crossref enrichment data."""

from __future__ import annotations

from typing import Any

import pandas as pd

from bioetl.core.logger import UnifiedLogger

__all__ = ["CROSSREF_MERGE_KEYS", "merge_crossref_with_base"]

logger = UnifiedLogger.get(__name__)

CROSSREF_MERGE_KEYS: dict[str, Any] = {
    "primary": "doi_clean",
    "fallback": ("crossref_doi", "crossref_doi_clean"),
}


def _normalise_doi(series: pd.Series) -> pd.Series:
    """Return a normalised lowercase DOI series suitable for joins."""

    values = series.astype("string")
    values = values.str.strip().str.lower()
    return values.where(series.notna(), None)


def merge_crossref_with_base(
    base_df: pd.DataFrame,
    crossref_df: pd.DataFrame,
    *,
    base_doi_column: str = "chembl_doi",
    conflict_detection: bool = True,
) -> pd.DataFrame:
    """Merge Crossref normalized data into ``base_df`` using DOI keys."""

    if crossref_df.empty:
        return base_df.copy()

    if base_doi_column not in base_df.columns:
        logger.warning("crossref_merge_missing_base_key", base_columns=list(base_df.columns))
        return base_df.copy()

    working = crossref_df.copy()

    if "doi_clean" not in working.columns:
        candidate = None
        for key in ("crossref_doi", "DOI", "doi"):
            if key in working.columns:
                candidate = working[key]
                break
        if candidate is None:
            logger.warning(
                "crossref_merge_missing_doi",
                crossref_columns=list(working.columns),
            )
            return base_df.copy()
        working["doi_clean"] = candidate

    # Prefixed rename while preserving already-prefixed columns
    rename_map: dict[str, str] = {}
    for column in working.columns:
        if column == "doi_clean":
            rename_map[column] = "crossref_doi_clean"
            continue
        if column.startswith("crossref_"):
            continue
        rename_map[column] = f"crossref_{column}"

    crossref_prefixed = working.rename(columns=rename_map)

    if "crossref_doi" not in crossref_prefixed.columns:
        # Ensure DOI column available for precedence logic
        doi_source = crossref_prefixed.get("crossref_doi_clean", working.get("doi_clean"))
        crossref_prefixed["crossref_doi"] = doi_source

    join_basis = crossref_prefixed.get("crossref_doi_clean")
    if join_basis is None:
        join_basis = crossref_prefixed["crossref_doi"]

    # Build join keys using normalised DOI representations
    base_join = _normalise_doi(base_df[base_doi_column])
    crossref_join = _normalise_doi(join_basis)

    merged = base_df.copy()
    merged["_crossref_join_key"] = base_join
    crossref_prefixed = crossref_prefixed.assign(_crossref_join_key=crossref_join)

    merged = merged.merge(
        crossref_prefixed,
        on="_crossref_join_key",
        how="left",
    )

    merged = merged.drop(columns="_crossref_join_key")

    if conflict_detection:
        merged = _annotate_conflicts(merged, base_doi_column)

    return merged


def _annotate_conflicts(merged: pd.DataFrame, base_doi_column: str) -> pd.DataFrame:
    """Add ``conflict_crossref_doi`` flag when DOIs mismatch after merge."""

    column = "conflict_crossref_doi"
    if column in merged.columns:
        return merged

    merged[column] = False
    if base_doi_column not in merged.columns or "crossref_doi" not in merged.columns:
        return merged

    base_series = merged[base_doi_column]
    crossref_series = merged["crossref_doi"]

    mask = base_series.notna() & crossref_series.notna()
    if not mask.any():
        return merged

    base_norm = _normalise_doi(base_series[mask])
    crossref_norm = _normalise_doi(crossref_series[mask])
    conflicts = base_norm != crossref_norm

    merged.loc[mask, column] = conflicts
    merged[column] = merged[column].astype("boolean")
    return merged
