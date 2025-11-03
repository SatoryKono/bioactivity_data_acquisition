"""Merge helpers for combining OpenAlex enrichment with base datasets."""

from __future__ import annotations

from collections.abc import Hashable

import pandas as pd

from bioetl.core.logger import UnifiedLogger

__all__ = ["OPENALEX_MERGE_KEYS", "merge_openalex_with_base"]

logger = UnifiedLogger.get(__name__)

OPENALEX_MERGE_KEYS: dict[str, Hashable | tuple[Hashable, ...]] = {
    "primary": "doi_clean",
    "fallback": ("openalex_doi", "openalex_id", "openalex_pmid"),
}


def merge_openalex_with_base(
    base_df: pd.DataFrame,
    openalex_df: pd.DataFrame,
    *,
    base_doi_column: str = "chembl_doi",
    base_pmid_column: str | None = "chembl_pmid",
    conflict_detection: bool = True,
) -> pd.DataFrame:
    """Merge OpenAlex enrichment dataframe into ``base_df``.

    The helper performs a left join on ``base_df`` preserving row order while
    deduplicating OpenAlex rows by ``doi_clean``.  When the DOI is missing the
    function falls back to the OpenAlex identifier or PMID when available.
    ``openalex_df`` is not mutated; a copy is created internally before
    renaming/prefixing columns.
    """

    if base_df.empty or openalex_df.empty:
        return base_df.copy()

    prefixed = _prepare_openalex_columns(openalex_df)

    if prefixed.empty:
        return base_df.copy()

    join_keys: list[tuple[str, str]] = []
    if base_doi_column in base_df.columns and "openalex_doi_clean" in prefixed.columns:
        join_keys.append((base_doi_column, "openalex_doi_clean"))

    if base_pmid_column and base_pmid_column in base_df.columns and "openalex_pmid" in prefixed.columns:
        join_keys.append((base_pmid_column, "openalex_pmid"))

    if "openalex_id" in prefixed.columns and base_doi_column in base_df.columns:
        join_keys.append((base_doi_column, "openalex_id"))

    base_working = base_df.copy()
    if base_pmid_column and base_pmid_column in base_working.columns:
        base_working[base_pmid_column] = pd.to_numeric(
            base_working[base_pmid_column], errors="coerce"
        ).astype("Int64")

    if not join_keys:
        logger.warning(
            "openalex_merge_no_keys",
            base_columns=list(base_df.columns),
            openalex_columns=list(prefixed.columns),
        )
        return base_working

    last_candidate = base_working.merge(
        prefixed,
        left_on=join_keys[0][0],
        right_on=join_keys[0][1],
        how="left",
    )

    if last_candidate[join_keys[0][1]].notna().any():
        result = last_candidate
    else:
        result = last_candidate
        for left_key, right_key in join_keys[1:]:
            candidate = base_working.merge(prefixed, left_on=left_key, right_on=right_key, how="left")
            last_candidate = candidate
            if candidate[right_key].notna().any():
                result = candidate
                break
        else:
            result = last_candidate

    if conflict_detection:
        result = _detect_openalex_conflicts(result, base_doi_column)

    return result


def _prepare_openalex_columns(openalex_df: pd.DataFrame) -> pd.DataFrame:
    """Return a prefixed OpenAlex dataframe suitable for joining."""

    working = openalex_df.copy()
    if working.empty:
        return working

    missing_doi = False
    if "doi_clean" not in working.columns:
        if "openalex_doi" in working.columns:
            working["doi_clean"] = working["openalex_doi"]
        else:
            missing_doi = True

    rename_map: dict[str, str] = {}
    for column in working.columns:
        if column == "doi_clean":
            continue
        if column.startswith("openalex_"):
            rename_map[column] = column
        else:
            rename_map[column] = f"openalex_{column}"

    prefixed = working.rename(columns=rename_map)

    if "doi_clean" in working.columns:
        prefixed["openalex_doi_clean"] = working["doi_clean"].astype(str).str.strip()
        if "openalex_doi" not in prefixed.columns:
            prefixed["openalex_doi"] = prefixed["openalex_doi_clean"]

    if "openalex_doi_clean" in prefixed.columns:
        prefixed = prefixed.drop_duplicates(subset=["openalex_doi_clean"], keep="first")
    elif missing_doi:
        logger.warning("openalex_merge_missing_doi", columns=list(working.columns))

    if "openalex_publication_date" in prefixed.columns:
        parsed_dates = pd.to_datetime(prefixed["openalex_publication_date"], errors="coerce")
        if "openalex_year" in prefixed.columns:
            existing_year = pd.to_numeric(prefixed["openalex_year"], errors="coerce").astype("Int64")
            prefixed["openalex_year"] = existing_year.fillna(parsed_dates.dt.year.astype("Int64"))
        else:
            prefixed["openalex_year"] = parsed_dates.dt.year.astype("Int64")

        prefixed["openalex_month"] = parsed_dates.dt.month.astype("Int64")
        prefixed["openalex_day"] = parsed_dates.dt.day.astype("Int64")

    if "openalex_is_oa" in prefixed.columns:
        prefixed["openalex_is_oa"] = prefixed["openalex_is_oa"].astype("boolean")

    if "openalex_pmid" in prefixed.columns:
        prefixed["openalex_pmid"] = pd.to_numeric(prefixed["openalex_pmid"], errors="coerce").astype("Int64")

    if "openalex_concepts_top3" in prefixed.columns:
        prefixed["openalex_concepts_top3"] = prefixed["openalex_concepts_top3"].apply(
            lambda value: value
            if isinstance(value, list)
            else ([] if pd.isna(value) else [value])
        )

    if "openalex_landing_page" not in prefixed.columns and "openalex_landing_page_url" in prefixed.columns:
        prefixed = prefixed.rename(columns={"openalex_landing_page_url": "openalex_landing_page"})

    if "openalex_doc_type" not in prefixed.columns and "openalex_type" in prefixed.columns:
        prefixed["openalex_doc_type"] = prefixed["openalex_type"]

    return prefixed


def _detect_openalex_conflicts(
    merged_df: pd.DataFrame,
    base_doi_column: str,
) -> pd.DataFrame:
    """Attach a ``conflict_openalex_doi`` flag highlighting DOI mismatches."""

    if base_doi_column not in merged_df.columns or "openalex_doi" not in merged_df.columns:
        return merged_df

    conflict_column = "conflict_openalex_doi"
    if conflict_column not in merged_df.columns:
        merged_df[conflict_column] = False

    base_series = merged_df[base_doi_column].astype(str).str.strip()
    oa_series = merged_df["openalex_doi"].astype(str).str.strip()

    mask = base_series.notna() & base_series.astype(bool) & oa_series.notna() & oa_series.astype(bool)
    merged_df.loc[mask, conflict_column] = base_series.loc[mask] != oa_series.loc[mask]
    merged_df[conflict_column] = merged_df[conflict_column].astype("boolean")
    return merged_df
