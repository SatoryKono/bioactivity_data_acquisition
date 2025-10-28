"""Functions for merging enriched document data from multiple sources."""

import pandas as pd

from bioetl.core.logger import UnifiedLogger

logger = UnifiedLogger.get(__name__)


def choose_field(row: pd.Series, field_name: str, precedence: list[str]) -> pd.Series:
    """Select field value by source precedence."""
    for source in precedence:
        col_name = f"{field_name}_{source}"
        if col_name in row.index:
            value = row[col_name]
            if pd.notna(value) and value != "":
                # Set the final field value
                row[field_name] = value
                # Record the source
                row[f"{field_name}_source"] = source
                return row

    # No value found from any source
    row[field_name] = None
    row[f"{field_name}_source"] = None
    return row


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
        # Join by PMID
        if "pubmed_id" in pubmed_df.columns and "chembl_pmid" in merged_df.columns:
            # Add prefix to all columns
            pubmed_prefixed = pubmed_df.add_prefix("pubmed_")
            merged_df = merged_df.merge(
                pubmed_prefixed,
                left_on="chembl_pmid",
                right_on="pubmed_pubmed_id",
                how="left",
            )

    if crossref_df is not None and not crossref_df.empty:
        # Join by DOI
        if "doi_clean" in crossref_df.columns and "chembl_doi" in merged_df.columns:
            # Add prefix to all columns
            crossref_prefixed = crossref_df.add_prefix("crossref_")
            # Fix double prefix: use crossref_doi_clean as crossref_doi
            if "crossref_doi_clean" in crossref_prefixed.columns:
                crossref_prefixed["crossref_doi"] = crossref_prefixed["crossref_doi_clean"]

            merged_df = merged_df.merge(
                crossref_prefixed,
                left_on="chembl_doi",
                right_on="crossref_doi_clean",
                how="left",
                suffixes=("", "_crossref"),
            )

    if openalex_df is not None and not openalex_df.empty:
        # Join by DOI
        if "doi_clean" in openalex_df.columns and "chembl_doi" in merged_df.columns:
            # Add prefix to all columns
            openalex_prefixed = openalex_df.add_prefix("openalex_")
            # Fix double prefix: use openalex_doi_clean as openalex_doi
            if "openalex_doi_clean" in openalex_prefixed.columns:
                openalex_prefixed["openalex_doi"] = openalex_prefixed["openalex_doi_clean"]

            merged_df = merged_df.merge(
                openalex_prefixed,
                left_on="chembl_doi",
                right_on="openalex_doi_clean",
                how="left",
                suffixes=("", "_openalex"),
            )

    if semantic_scholar_df is not None and not semantic_scholar_df.empty:
        # Add prefix to all columns first
        ss_prefixed = semantic_scholar_df.add_prefix("semantic_scholar_")
        logger.info("semantic_scholar_columns", columns=list(ss_prefixed.columns))

        # Join by DOI if available
        has_doi = "semantic_scholar_doi_clean" in ss_prefixed.columns and "chembl_doi" in merged_df.columns
        has_pmid = "semantic_scholar_pubmed_id" in ss_prefixed.columns and "chembl_pmid" in merged_df.columns

        logger.info("semantic_scholar_merge_check", has_doi=bool(has_doi), has_pmid=bool(has_pmid))

        if has_doi:
            # Rename doi_clean to semantic_scholar_doi after merge
            merged_df = merged_df.merge(
                ss_prefixed,
                left_on="chembl_doi",
                right_on="semantic_scholar_doi_clean",
                how="left",
            )
            # Rename semantic_scholar_doi_clean to semantic_scholar_doi for schema compliance
            if "semantic_scholar_doi_clean" in merged_df.columns:
                merged_df = merged_df.rename(columns={"semantic_scholar_doi_clean": "semantic_scholar_doi"})
            
            matched = merged_df["semantic_scholar_paper_id"].notna().sum() if "semantic_scholar_paper_id" in merged_df.columns else 0
            logger.info("semantic_scholar_merged_by_doi", matched=matched)
        elif has_pmid:
            merged_df = merged_df.merge(
                ss_prefixed,
                left_on="chembl_pmid",
                right_on="semantic_scholar_pubmed_id",
                how="left",
            )
            matched = merged_df["semantic_scholar_paper_id"].notna().sum() if "semantic_scholar_paper_id" in merged_df.columns else 0
            logger.info("semantic_scholar_merged_by_pmid", matched=matched)
        else:
            logger.warning("semantic_scholar_no_merge_keys")

        # Note: We keep all source-prefixed fields without applying precedence rules
        # All fields from adapters are preserved with their source prefixes

    return merged_df


def detect_conflicts(row: pd.Series) -> pd.Series:
    """Detect DOI/PMID conflicts between sources."""
    # Check DOI conflicts
    dois = []
    for source in ["chembl", "pubmed", "crossref"]:
        col = f"{source}_doi"
        if col in row.index and pd.notna(row[col]) and row[col] != "":
            dois.append(str(row[col]))

    if len(set(dois)) > 1:
        row["conflict_doi"] = True
    else:
        row["conflict_doi"] = False

    # Check PMID conflicts
    pmids = []
    for source in ["chembl", "pubmed"]:
        col = f"{source}_pmid"
        if col in row.index and pd.notna(row[col]) and row[col] != 0:
            pmids.append(int(row[col]))

    if len(set(pmids)) > 1:
        row["conflict_pmid"] = True
    else:
        row["conflict_pmid"] = False

    return row

