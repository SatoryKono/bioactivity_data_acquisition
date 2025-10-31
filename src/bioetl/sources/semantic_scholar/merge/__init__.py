"""Merge policy helpers for Semantic Scholar enrichment payloads."""

from __future__ import annotations

from typing import Any

import pandas as pd

from bioetl.core.logger import UnifiedLogger

__all__ = [
    "SEMANTIC_SCHOLAR_MERGE_KEYS",
    "merge_semantic_scholar_with_base",
]

logger = UnifiedLogger.get(__name__)

SEMANTIC_SCHOLAR_MERGE_KEYS: dict[str, str] = {
    "primary": "doi_clean",
    "secondary": "semantic_scholar_pmid",
    "title": "semantic_scholar_title_for_join",
}


def merge_semantic_scholar_with_base(
    base_df: pd.DataFrame,
    semantic_scholar_df: pd.DataFrame,
    *,
    base_doi_column: str = "chembl_doi",
    base_pmid_column: str | None = "chembl_pmid",
    base_title_column: str | None = "chembl_title",
    conflict_detection: bool = True,
) -> pd.DataFrame:
    """Merge Semantic Scholar enrichment dataframe into *base_df*."""

    if base_df.empty or semantic_scholar_df.empty:
        return base_df.copy()

    working = semantic_scholar_df.copy()

    if "doi_clean" not in working.columns:
        doi_source = next((col for col in ("doi", "paper_id") if col in working.columns), None)
        if doi_source is not None:
            working["doi_clean"] = working[doi_source]
        else:
            logger.warning("semantic_scholar_no_doi_column", columns=list(working.columns))
            working["doi_clean"] = pd.Series([pd.NA] * len(working), dtype="string")

    rename_map = {
        column: f"semantic_scholar_{column.lstrip('_')}"
        for column in working.columns
        if column != "doi_clean"
    }
    prefixed = working.rename(columns=rename_map)

    if "semantic_scholar_pubmed_id" in prefixed.columns:
        prefixed["semantic_scholar_pmid"] = pd.to_numeric(
            prefixed["semantic_scholar_pubmed_id"],
            errors="coerce",
        ).astype("Int64")

    prefixed["semantic_scholar_doi"] = working["doi_clean"]
    prefixed["doi_clean"] = working["doi_clean"]

    for column in (
        "semantic_scholar_year",
        "semantic_scholar_citation_count",
        "semantic_scholar_influential_citations",
        "semantic_scholar_reference_count",
    ):
        if column in prefixed.columns:
            prefixed[column] = pd.to_numeric(prefixed[column], errors="coerce").astype("Int64")

    if "semantic_scholar_is_oa" in prefixed.columns:
        prefixed["semantic_scholar_is_oa"] = prefixed["semantic_scholar_is_oa"].astype("boolean")

    for column in ("semantic_scholar_publication_types", "semantic_scholar_fields_of_study"):
        if column in prefixed.columns:
            prefixed[column] = prefixed[column].apply(_ensure_list)

    joined = base_df.copy()

    if base_doi_column and base_doi_column in joined.columns and _has_merge_values(joined[base_doi_column]):
        joined = joined.merge(
            prefixed,
            left_on=base_doi_column,
            right_on="doi_clean",
            how="left",
        )
    elif (
        base_pmid_column
        and base_pmid_column in joined.columns
        and "semantic_scholar_pmid" in prefixed.columns
        and _has_merge_values(joined[base_pmid_column])
    ):
        left = joined.copy()
        left[base_pmid_column] = pd.to_numeric(left[base_pmid_column], errors="coerce").astype("Int64")
        joined = left.merge(
            prefixed,
            left_on=base_pmid_column,
            right_on="semantic_scholar_pmid",
            how="left",
        )
    elif (
        base_title_column
        and base_title_column in joined.columns
        and "semantic_scholar_title_for_join" in prefixed.columns
    ):
        left = joined.copy()
        left["_semantic_scholar_title_key"] = left[base_title_column].astype(str).str.strip().str.lower()
        right = prefixed.copy()
        right["_semantic_scholar_title_key"] = (
            right["semantic_scholar_title_for_join"].astype(str).str.strip().str.lower()
        )
        joined = left.merge(
            right.drop_duplicates("_semantic_scholar_title_key"),
            on="_semantic_scholar_title_key",
            how="left",
        )
        joined = joined.drop(columns=["_semantic_scholar_title_key"])
    else:
        logger.warning("semantic_scholar_no_join_keys", base_columns=list(joined.columns))
        return joined

    if "doi_clean" in joined.columns:
        joined = joined.drop(columns=["doi_clean"])

    if "semantic_scholar_title_for_join" in joined.columns:
        joined = joined.drop(columns=["semantic_scholar_title_for_join"])

    if "semantic_scholar_pubmed_id" in joined.columns:
        joined = joined.drop(columns=["semantic_scholar_pubmed_id"])

    if conflict_detection:
        joined = _detect_conflicts(joined, base_doi_column, base_pmid_column)

    return joined


def _has_merge_values(series: pd.Series) -> bool:
    """Return ``True`` when *series* contains at least one usable value."""

    if series.empty:
        return False
    if pd.api.types.is_string_dtype(series):
        normalized = series.astype(str).str.strip()
        return normalized.replace({"": pd.NA}).notna().any()
    return series.notna().any()


def _ensure_list(value: Any) -> list[Any] | None:
    """Ensure *value* is represented as a list for downstream schema validation."""

    if value is None or value is pd.NA:
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _detect_conflicts(
    merged_df: pd.DataFrame,
    base_doi_column: str | None,
    base_pmid_column: str | None,
) -> pd.DataFrame:
    """Detect identifier conflicts between ChEMBL and Semantic Scholar."""

    if "conflict_semantic_scholar_doi" not in merged_df.columns:
        merged_df["conflict_semantic_scholar_doi"] = False
    if "conflict_semantic_scholar_pmid" not in merged_df.columns:
        merged_df["conflict_semantic_scholar_pmid"] = False

    if base_doi_column and base_doi_column in merged_df.columns and "semantic_scholar_doi" in merged_df.columns:
        base_values = merged_df[base_doi_column].astype(str).str.strip()
        ss_values = merged_df["semantic_scholar_doi"].astype(str).str.strip()
        mask = base_values.notna() & ss_values.notna() & (base_values != ss_values)
        merged_df.loc[mask, "conflict_semantic_scholar_doi"] = True

    if base_pmid_column and base_pmid_column in merged_df.columns and "semantic_scholar_pmid" in merged_df.columns:
        base_pmid = pd.to_numeric(merged_df[base_pmid_column], errors="coerce").astype("Int64")
        ss_pmid = pd.to_numeric(merged_df["semantic_scholar_pmid"], errors="coerce").astype("Int64")
        mask = base_pmid.notna() & ss_pmid.notna() & (base_pmid != ss_pmid)
        merged_df.loc[mask, "conflict_semantic_scholar_pmid"] = True

    merged_df["conflict_semantic_scholar_doi"] = merged_df["conflict_semantic_scholar_doi"].astype("boolean")
    merged_df["conflict_semantic_scholar_pmid"] = merged_df["conflict_semantic_scholar_pmid"].astype("boolean")

    return merged_df
