"""Merge helpers for PubMed enrichment data."""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from bioetl.core.logger import UnifiedLogger

__all__ = ["PUBMED_MERGE_KEYS", "merge_pubmed_with_base"]


logger = UnifiedLogger.get(__name__)


PUBMED_MERGE_KEYS = {
    "primary": "pmid",
    "fallback": ("doi_clean", "pubmed_doi", "doi"),
}


def merge_pubmed_with_base(
    base_df: pd.DataFrame,
    pubmed_df: pd.DataFrame,
    *,
    base_pmid_column: str = "chembl_pmid",
    base_doi_column: str | None = "chembl_doi",
    conflict_detection: bool = True,
) -> pd.DataFrame:
    """Merge normalized PubMed data into a ChEMBL-centric dataframe."""

    if base_df.empty or pubmed_df.empty:
        return base_df.copy()

    prepared = _prepare_pubmed_dataframe(pubmed_df)

    if prepared.empty:
        return base_df.copy()

    merged = base_df.copy()

    if base_pmid_column in merged.columns:
        pmid_series = pd.to_numeric(merged[base_pmid_column], errors="coerce").astype("Int64")
        merged[base_pmid_column] = pmid_series
        merged = merged.merge(
            prepared,
            left_on=base_pmid_column,
            right_on="pmid",
            how="left",
            suffixes=("", "_pubmed"),
        )
    elif base_doi_column and base_doi_column in merged.columns:
        fallback_key = _first_existing(prepared, PUBMED_MERGE_KEYS["fallback"])
        if fallback_key is None:
            logger.warning("pubmed_no_fallback_key", available=list(prepared.columns))
            return merged
        merged = merged.merge(
            prepared,
            left_on=base_doi_column,
            right_on=fallback_key,
            how="left",
            suffixes=("", "_pubmed"),
        )
    else:
        logger.warning(
            "pubmed_no_join_keys",
            base_columns=list(merged.columns),
            base_pmid_column=base_pmid_column,
            base_doi_column=base_doi_column,
        )
        return merged

    if conflict_detection:
        merged = _annotate_conflicts(merged, base_pmid_column)

    return merged


def _prepare_pubmed_dataframe(pubmed_df: pd.DataFrame) -> pd.DataFrame:
    """Prefix and normalize PubMed columns required for merging."""

    required_column = PUBMED_MERGE_KEYS["primary"]
    candidate = pubmed_df.copy()

    if required_column not in candidate.columns:
        if "pubmed_pmid" in candidate.columns:
            candidate[required_column] = pd.to_numeric(candidate["pubmed_pmid"], errors="coerce")
        else:
            logger.warning("pubmed_missing_pmid", columns=list(candidate.columns))
            return pd.DataFrame(columns=[required_column])

    candidate[required_column] = pd.to_numeric(candidate[required_column], errors="coerce").astype("Int64")

    prefixed = candidate.drop(columns=[required_column], errors="ignore").add_prefix("pubmed_")

    rename_map: dict[str, str] = {}
    for column in prefixed.columns:
        if column.startswith("pubmed_pubmed_"):
            rename_map[column] = "pubmed_" + column.replace("pubmed_pubmed_", "")

    if "pubmed_title" in prefixed.columns:
        rename_map["pubmed_title"] = "pubmed_article_title"

    prefixed = prefixed.rename(columns=rename_map)

    pmid_series = pd.to_numeric(candidate[required_column], errors="coerce").astype("Int64")
    prefixed["pmid"] = pmid_series

    if "doi_clean" in candidate.columns:
        prefixed["doi_clean"] = candidate["doi_clean"]
    elif "pubmed_doi" in prefixed.columns and "doi_clean" not in prefixed.columns:
        prefixed["doi_clean"] = prefixed["pubmed_doi"]

    return prefixed


def _first_existing(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    for column in candidates:
        if column in df.columns:
            return column
    return None


def _annotate_conflicts(merged: pd.DataFrame, base_pmid_column: str) -> pd.DataFrame:
    """Flag conflicting PubMed PMIDs between sources."""

    conflict_column = "conflict_pubmed_pmid"
    if conflict_column in merged.columns:
        return merged

    merged[conflict_column] = False

    if base_pmid_column in merged.columns and "pubmed_pmid" in merged.columns:
        base_series = pd.to_numeric(merged[base_pmid_column], errors="coerce").astype("Int64")
        pubmed_series = pd.to_numeric(merged["pubmed_pmid"], errors="coerce").astype("Int64")

        conflict_mask = base_series.notna() & pubmed_series.notna() & (base_series != pubmed_series)
        if conflict_mask.any():
            merged.loc[conflict_mask, conflict_column] = True

    return merged
