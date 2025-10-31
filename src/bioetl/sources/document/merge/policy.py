"""Functions for merging enriched document data from multiple sources."""

from collections.abc import Callable

import pandas as pd

from bioetl.core.logger import UnifiedLogger
from bioetl.sources.crossref.merge import merge_crossref_with_base
from bioetl.sources.openalex.merge import merge_openalex_with_base
from bioetl.sources.pubmed.merge import merge_pubmed_with_base
from bioetl.sources.semantic_scholar.merge import merge_semantic_scholar_with_base

__all__ = [
    "FIELD_PRECEDENCE",
    "CASTERS",
    "merge_with_precedence",
    "detect_conflicts",
]


FIELD_PRECEDENCE: dict[str, list[tuple[str, str]]] = {
    "doi_clean": [
        ("crossref", "crossref_doi"),
        ("pubmed", "pubmed_doi"),
        ("openalex", "openalex_doi"),
        ("semantic_scholar", "semantic_scholar_doi"),
        ("chembl", "chembl_doi"),
    ],
    "pmid": [
        ("pubmed", "pubmed_pmid"),
        ("chembl", "chembl_pmid"),
        ("openalex", "openalex_pmid"),
        ("semantic_scholar", "semantic_scholar_pmid"),
    ],
    "title": [
        ("pubmed", "pubmed_article_title"),
        ("chembl", "chembl_title"),
        ("openalex", "openalex_title"),
        ("crossref", "crossref_title"),
        ("semantic_scholar", "semantic_scholar_title"),
    ],
    "abstract": [
        ("pubmed", "pubmed_abstract"),
        ("chembl", "chembl_abstract"),
        ("semantic_scholar", "semantic_scholar_abstract"),
    ],
    "journal": [
        ("pubmed", "pubmed_journal"),
        ("crossref", "crossref_journal"),
        ("openalex", "openalex_journal"),
        ("chembl", "chembl_journal"),
        ("semantic_scholar", "semantic_scholar_journal"),
    ],
    "journal_abbrev": [
        ("pubmed", "pubmed_journal_abbrev"),
        ("chembl", "chembl_journal_abbrev"),
    ],
    "authors": [
        ("pubmed", "pubmed_authors"),
        ("crossref", "crossref_authors"),
        ("openalex", "openalex_authors"),
        ("chembl", "chembl_authors"),
        ("semantic_scholar", "semantic_scholar_authors"),
    ],
    "year": [
        ("pubmed", "pubmed_year"),
        ("crossref", "crossref_year"),
        ("openalex", "openalex_year"),
        ("semantic_scholar", "semantic_scholar_year"),
        ("chembl", "chembl_year"),
    ],
    "volume": [
        ("pubmed", "pubmed_volume"),
        ("crossref", "crossref_volume"),
        ("chembl", "chembl_volume"),
    ],
    "issue": [
        ("pubmed", "pubmed_issue"),
        ("crossref", "crossref_issue"),
        ("chembl", "chembl_issue"),
    ],
    "first_page": [
        ("pubmed", "pubmed_first_page"),
        ("crossref", "crossref_first_page"),
    ],
    "last_page": [
        ("pubmed", "pubmed_last_page"),
        ("crossref", "crossref_last_page"),
    ],
    "issn_print": [
        ("crossref", "crossref_issn_print"),
        ("pubmed", "pubmed_issn_print"),
        ("openalex", "openalex_issn"),
        ("semantic_scholar", "semantic_scholar_issn"),
    ],
    "issn_electronic": [
        ("crossref", "crossref_issn_electronic"),
        ("pubmed", "pubmed_issn_electronic"),
        ("openalex", "openalex_issn"),
        ("semantic_scholar", "semantic_scholar_issn"),
    ],
    "is_oa": [
        ("openalex", "openalex_is_oa"),
        ("semantic_scholar", "semantic_scholar_is_oa"),
    ],
    "oa_status": [
        ("openalex", "openalex_oa_status"),
        ("semantic_scholar", "semantic_scholar_oa_status"),
    ],
    "oa_url": [
        ("openalex", "openalex_oa_url"),
        ("semantic_scholar", "semantic_scholar_oa_url"),
    ],
    "citation_count": [
        ("semantic_scholar", "semantic_scholar_citation_count"),
        ("openalex", "openalex_citation_count"),
    ],
    "influential_citations": [
        ("semantic_scholar", "semantic_scholar_influential_citations"),
    ],
    "fields_of_study": [
        ("semantic_scholar", "semantic_scholar_fields_of_study"),
    ],
    "concepts_top3": [
        ("openalex", "openalex_concepts_top3"),
    ],
    "mesh_terms": [
        ("pubmed", "pubmed_mesh_descriptors"),
    ],
    "chemicals": [
        ("pubmed", "pubmed_chemical_list"),
    ],
}

CASTERS: dict[str, Callable[[pd.Series], pd.Series]] = {
    "pmid": lambda s: pd.to_numeric(s, errors="coerce").astype("Int64"),
    "year": lambda s: pd.to_numeric(s, errors="coerce").astype("Int64"),
    "citation_count": lambda s: pd.to_numeric(s, errors="coerce").astype("Int64"),
    "influential_citations": lambda s: pd.to_numeric(s, errors="coerce").astype("Int64"),
    "is_oa": lambda s: s.astype("boolean"),
}

logger = UnifiedLogger.get(__name__)


def _normalize_output_value(value: object) -> object:
    """Normalize complex values (lists, tuples) to a deterministic representation."""
    if isinstance(value, list | tuple):
        filtered = [str(v) for v in value if v not in (None, "")]
        if not filtered:
            return None
        return "; ".join(filtered)
    return value


def _has_value(series: pd.Series) -> pd.Series:
    """Return mask where series contains a non-empty value."""
    mask = series.notna()
    if series.dtype == object:
        mask &= series.astype(str).str.strip() != ""
    return mask


def _needs_value(series: pd.Series) -> pd.Series:
    """Return mask where the destination series still requires a value."""
    mask = series.isna()
    if series.dtype == object:
        mask |= series.astype(str).str.strip() == ""
    return mask


def _apply_field_precedence(merged_df: pd.DataFrame) -> pd.DataFrame:
    """Apply precedence rules for each aggregated field and annotate source."""
    if merged_df.empty:
        # Ensure all aggregated columns exist even for empty frames
        for field in FIELD_PRECEDENCE:
            merged_df[field] = pd.Series(dtype="object")
            merged_df[f"{field}_source"] = pd.Series(dtype="object")
        return merged_df

    for field, candidates in FIELD_PRECEDENCE.items():
        value_column = field
        source_column = f"{field}_source"

        if value_column not in merged_df.columns:
            merged_df[value_column] = pd.Series([pd.NA] * len(merged_df), dtype="object")
        if source_column not in merged_df.columns:
            merged_df[source_column] = pd.Series([pd.NA] * len(merged_df), dtype="object")

        for source_name, candidate_col in candidates:
            if candidate_col not in merged_df.columns:
                continue

            candidate_series = merged_df[candidate_col]
            candidate_mask = _has_value(candidate_series)
            target_mask = _needs_value(merged_df[value_column])
            update_mask = candidate_mask & target_mask

            if update_mask.any():
                merged_df.loc[update_mask, value_column] = candidate_series[update_mask].apply(
                    _normalize_output_value
                )
                merged_df.loc[update_mask, source_column] = source_name

        caster = CASTERS.get(field)
        if caster is not None:
            merged_df[value_column] = caster(merged_df[value_column])

    return merged_df


def merge_with_precedence(
    chembl_df: pd.DataFrame,
    pubmed_df: pd.DataFrame | None = None,
    crossref_df: pd.DataFrame | None = None,
    openalex_df: pd.DataFrame | None = None,
    semantic_scholar_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Merge document data from multiple sources with field-level precedence."""

    # Start with ChEMBL as base
    merged_df = chembl_df.copy()

    # Merge each source by their join key
    if pubmed_df is not None and not pubmed_df.empty:
        merged_df = merge_pubmed_with_base(
            merged_df,
            pubmed_df,
            base_pmid_column="chembl_pmid",
            base_doi_column="chembl_doi",
            conflict_detection=False,
        )

    if crossref_df is not None and not crossref_df.empty:
        merged_df = merge_crossref_with_base(
            merged_df,
            crossref_df,
            base_doi_column="chembl_doi",
        )

    if openalex_df is not None and not openalex_df.empty:
        merged_df = merge_openalex_with_base(
            merged_df,
            openalex_df,
            base_doi_column="chembl_doi",
            base_pmid_column="chembl_pmid",
            conflict_detection=False,
        )

    if semantic_scholar_df is not None and not semantic_scholar_df.empty:
        title_column = None
        if "_original_title" in merged_df.columns:
            title_column = "_original_title"
        elif "chembl_title" in merged_df.columns:
            title_column = "chembl_title"

        merged_df = merge_semantic_scholar_with_base(
            merged_df,
            semantic_scholar_df,
            base_title_column=title_column,
        )

    merged_df = _apply_field_precedence(merged_df)

    if merged_df.empty:
        merged_df["conflict_doi"] = pd.Series(dtype="boolean")
        merged_df["conflict_pmid"] = pd.Series(dtype="boolean")
        return merged_df

    merged_df = merged_df.apply(detect_conflicts, axis=1)
    merged_df["conflict_doi"] = merged_df["conflict_doi"].astype("boolean")
    merged_df["conflict_pmid"] = merged_df["conflict_pmid"].astype("boolean")

    return merged_df


def detect_conflicts(row: pd.Series) -> pd.Series:
    """Detect DOI/PMID conflicts between sources."""

    doi_columns = [
        col
        for col in [
            "chembl_doi",
            "pubmed_doi",
            "crossref_doi",
            "openalex_doi",
            "semantic_scholar_doi",
        ]
        if col in row.index
    ]
    doi_values = {
        str(row[col]).strip()
        for col in doi_columns
        if pd.notna(row[col]) and str(row[col]).strip()
    }
    row["conflict_doi"] = len(doi_values) > 1

    pmid_columns = [
        col
        for col in [
            "chembl_pmid",
            "pubmed_pmid",
            "openalex_pmid",
            "semantic_scholar_pmid",
        ]
        if col in row.index
    ]
    pmid_values: set[str] = set()
    for col in pmid_columns:
        value = row[col]
        if pd.isna(value):
            continue
        try:
            pmid_int = int(str(value).strip())
        except (TypeError, ValueError):
            continue
        if pmid_int == 0:
            continue
        pmid_values.add(str(pmid_int))
    row["conflict_pmid"] = len(pmid_values) > 1

    return row

