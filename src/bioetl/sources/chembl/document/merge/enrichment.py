"""Utilities for merging ChEMBL data with external enrichment payloads."""

from __future__ import annotations


import pandas as pd

from bioetl.core.logger import UnifiedLogger
from bioetl.sources.document.merge.policy import merge_with_precedence

logger = UnifiedLogger.get(__name__)


def merge_enrichment_results(
    chembl_df: pd.DataFrame,
    *,
    pubmed_df: pd.DataFrame | None = None,
    crossref_df: pd.DataFrame | None = None,
    openalex_df: pd.DataFrame | None = None,
    semantic_scholar_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Merge ChEMBL base dataframe with optional enrichment sources."""

    logger.info(
        "before_merge",
        chembl_cols=len(chembl_df.columns),
        chembl_rows=len(chembl_df),
    )

    for name, frame in (
        ("pubmed", pubmed_df),
        ("crossref", crossref_df),
        ("openalex", openalex_df),
        ("semantic_scholar", semantic_scholar_df),
    ):
        if frame is not None and not frame.empty:
            logger.info("adapter_df_size", source=name, rows=len(frame), cols=len(frame.columns))

    enriched_df = merge_with_precedence(
        chembl_df,
        pubmed_df,
        crossref_df,
        openalex_df,
        semantic_scholar_df,
    )

    logger.info(
        "after_merge",
        enriched_cols=len(enriched_df.columns),
        enriched_rows=len(enriched_df),
    )
    return enriched_df


__all__ = ["merge_enrichment_results"]
