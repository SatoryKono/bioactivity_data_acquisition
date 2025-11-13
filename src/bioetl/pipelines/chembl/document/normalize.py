"""Enrichment functions for Document pipeline."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd

from bioetl.clients.client_chembl_common import ChemblClient
from bioetl.core.io import ensure_columns
from bioetl.core.logging import LogEvents
from bioetl.core.logging import UnifiedLogger
from bioetl.schemas.chembl_document_enrichment import DOCUMENT_TERMS_ENRICHMENT_SCHEMA

__all__ = ["enrich_with_document_terms", "aggregate_terms", "_escape_pipe"]


_ensure_columns = ensure_columns


def _escape_pipe(value: str | Any) -> str:
    """Escape pipe and backslash delimiters in string values.

    Parameters
    ----------
    value:
        Input value to escape. ``None`` and NA become an empty string.

    Returns
    -------
    str:
        String with escaped delimiters: ``|`` → ``\\|``, ``\\`` → ``\\\\``.
    """

    if value is None or pd.isna(value):
        return ""

    text = str(value)
    if not text:
        return ""

    return text.replace("\\", "\\\\").replace("|", "\\|")


_DOCUMENT_TERM_COLUMNS: tuple[tuple[str, str], ...] = (
    ("term", "string"),
    ("weight", "string"),
)


def aggregate_terms(
    rows: Iterable[dict[str, Any]],
    sort: str = "weight_desc",
) -> dict[str, dict[str, str]]:
    """Aggregate document terms by document_chembl_id.

    Parameters
    ----------
    rows:
        Iterable of document_term records, each with 'document_chembl_id', 'term', 'weight'.
    sort:
        Sort order: 'weight_desc' (default) sorts by weight descending, None preserves order.

    Returns
    -------
    dict[str, dict[str, str]]:
        Dictionary keyed by document_chembl_id -> {'term': 't1|t2|...', 'weight': 'w1|w2|...'}.
        Terms and weights are serialized with "|" separator, order is synchronized.
    """
    bucket: dict[str, list[tuple[str, Any]]] = defaultdict(list)

    for r in rows:
        did = r.get("document_chembl_id")
        if not did:
            continue

        term_value = r.get("term")
        weight_value = r.get("weight")

        # Convert term to string, handle None
        term_str = str(term_value) if term_value is not None else ""

        # Keep weight as-is (can be number, string, or None)
        bucket[did].append((term_str, weight_value))

    result: dict[str, dict[str, str]] = {}

    for did, items in bucket.items():
        # Sort by weight (descending) if sort='weight_desc'
        if sort == "weight_desc":
            items.sort(
                key=lambda x: (
                    float(x[1]) if x[1] not in (None, "") and _is_numeric(x[1]) else float("-inf")
                ),
                reverse=True,
            )

        # Escape terms and join with "|"
        terms_list: list[str] = []
        weights_list: list[str] = []

        for term, weight in items:
            # Escape pipe in term
            escaped_term = _escape_pipe(term or "")
            terms_list.append(escaped_term)

            # Convert weight to string, handle None/empty
            if weight in (None, ""):
                weights_list.append("")
            else:
                weights_list.append(str(weight))

        result[did] = {
            "term": "|".join(terms_list),
            "weight": "|".join(weights_list),
        }

    return result


def _is_numeric(value: Any) -> bool:
    """Check if value can be converted to float."""
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def enrich_with_document_terms(
    df_docs: pd.DataFrame,
    client: ChemblClient,
    cfg: Mapping[str, Any],
) -> pd.DataFrame:
    """Enrich the document DataFrame with document_term fields.

    Parameters
    ----------
    df_docs:
        Document DataFrame; must contain `document_chembl_id`.
    client:
        ChemblClient used for ChEMBL API requests.
    cfg:
        Enrichment configuration from `config.chembl.document.enrich.document_term`.

    Returns
    -------
    pd.DataFrame:
        Enriched DataFrame with added columns:
        - term (nullable string, pipe-separated terms)
        - weight (nullable string, pipe-separated weights)
    """
    log = UnifiedLogger.get(__name__).bind(component="document_enrichment")

    def _ensure_term_columns(df_input: pd.DataFrame) -> pd.DataFrame:
        result_frame = df_input.copy()
        for column_name in ("term", "weight"):
            if column_name not in result_frame.columns:
                result_frame[column_name] = pd.Series(
                    ["" for _ in range(len(result_frame))],
                    index=result_frame.index,
                    dtype="string",
                )
            else:
                result_frame[column_name] = result_frame[column_name].astype("string")

            na_mask = result_frame[column_name].isna()
            if bool(na_mask.any()):
                result_frame.loc[na_mask, column_name] = ""
            result_frame[column_name] = result_frame[column_name].astype("string")
        return result_frame

    df_docs = _ensure_columns(df_docs, _DOCUMENT_TERM_COLUMNS)

    if df_docs.empty:
        log.debug(LogEvents.ENRICHMENT_SKIPPED_EMPTY_DATAFRAME)
        prepared_empty = _ensure_term_columns(df_docs)
        return DOCUMENT_TERMS_ENRICHMENT_SCHEMA.validate(prepared_empty, lazy=True)

    # Ensure required columns are present.
    required_cols = ["document_chembl_id"]
    missing_cols = [col for col in required_cols if col not in df_docs.columns]
    if missing_cols:
        log.warning(LogEvents.ENRICHMENT_SKIPPED_MISSING_COLUMNS,
            missing_columns=missing_cols,
        )
        prepared_missing = _ensure_term_columns(df_docs)
        return DOCUMENT_TERMS_ENRICHMENT_SCHEMA.validate(prepared_missing, lazy=True)

    # Collect unique document_chembl_id values, dropping NA.
    doc_ids: list[str] = []
    for _, row in df_docs.iterrows():
        doc_id = row.get("document_chembl_id")

        # Skip NaN/None values.
        if pd.isna(doc_id) or doc_id is None:
            continue

        # Convert to string.
        doc_id_str = str(doc_id).strip()

        if doc_id_str:
            doc_ids.append(doc_id_str)

    if not doc_ids:
        log.debug(LogEvents.ENRICHMENT_SKIPPED_NO_VALID_IDS)
        return DOCUMENT_TERMS_ENRICHMENT_SCHEMA.validate(df_docs, lazy=True)

    # Retrieve configuration.
    fields = cfg.get("select_fields", ["document_chembl_id", "term", "weight"])
    page_limit = cfg.get("page_limit", 1000)
    sort = cfg.get("sort", "weight_desc")

    # Call client.fetch_document_terms_by_ids.
    log.info(LogEvents.ENRICHMENT_FETCHING_TERMS, ids_count=len(set(doc_ids)))
    records_dict = client.fetch_document_terms_by_ids(
        ids=doc_ids,
        fields=list(fields),
        page_limit=page_limit,
    )

    # Flatten dict[doc_id, list[records]] into rows for aggregate_terms.
    all_records: list[dict[str, Any]] = []
    for records_list in records_dict.values():
        all_records.extend(records_list)

    # Aggregate terms.
    agg_result = aggregate_terms(all_records, sort=sort)

    # Build DataFrame for the join.
    enrichment_data: list[dict[str, Any]] = []
    for doc_id, term_weight in agg_result.items():
        enrichment_data.append(
            {
                "document_chembl_id": doc_id,
                "term": term_weight["term"],
                "weight": term_weight["weight"],
            }
        )

    if not enrichment_data:
        log.debug(LogEvents.ENRICHMENT_NO_RECORDS_FOUND)
        prepared = _ensure_term_columns(df_docs)
        return DOCUMENT_TERMS_ENRICHMENT_SCHEMA.validate(prepared, lazy=True)

    df_enrich = pd.DataFrame(enrichment_data)

    # Left-join back to df_docs on document_chembl_id, preserving index order.
    original_index = df_docs.index.copy()
    df_result = df_docs.merge(
        df_enrich,
        on=["document_chembl_id"],
        how="left",
        suffixes=("", "_enrich"),
    )

    # Move values from *_enrich columns into main columns.
    for col in ["term", "weight"]:
        enrich_col = f"{col}_enrich"
        if enrich_col in df_result.columns:
            base_series = df_result[col] if col in df_result.columns else pd.Series([pd.NA] * len(df_result), index=df_result.index, dtype="object")
            enrich_series = df_result[enrich_col]
            df_result[col] = base_series.where(pd.notna(base_series), enrich_series)
            df_result = df_result.drop(columns=[enrich_col])

    # Ensure all expected columns exist (fill NA where missing).
    for col in ["term", "weight"]:
        if col not in df_result.columns:
            df_result[col] = pd.Series(
                ["" for _ in range(len(df_result))],
                index=df_result.index,
                dtype="string",
            )
        else:
            df_result[col] = df_result[col].astype("string")

        na_mask = df_result[col].isna()
        if bool(na_mask.any()):
            df_result.loc[na_mask, col] = ""
        df_result[col] = df_result[col].astype("string")

    # Restore original row order.
    df_result = df_result.reindex(original_index)
    df_result = _ensure_term_columns(df_result)

    log.info(LogEvents.ENRICHMENT_COMPLETED,
        rows_enriched=df_result.shape[0],
        documents_with_terms=len(agg_result),
    )
    return DOCUMENT_TERMS_ENRICHMENT_SCHEMA.validate(df_result, lazy=True)
