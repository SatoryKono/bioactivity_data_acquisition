"""Normalisation helpers for ChEMBL document payloads."""

from __future__ import annotations

from typing import Any, Mapping

import pandas as pd

from bioetl.core.logger import UnifiedLogger
from bioetl.normalizers import NormalizerRegistry, registry as normalizer_registry
from bioetl.utils.dtypes import coerce_optional_bool

logger = UnifiedLogger.get(__name__)


def normalize_document_frame(
    df: pd.DataFrame,
    *,
    registry: NormalizerRegistry = normalizer_registry,
) -> pd.DataFrame:
    """Vectorised wrapper applying :func:`_normalize_document_record` to a frame."""

    if df.empty:
        return df.copy()

    records = [_normalize_document_record(row, registry=registry) for row in df.to_dict(orient="records")]
    normalised = pd.DataFrame(records)
    return normalised.convert_dtypes()


def _normalize_document_record(
    record: Mapping[str, Any],
    *,
    registry: NormalizerRegistry,
) -> dict[str, Any]:
    """Normalise a single ChEMBL document record."""

    normalised = dict(record)

    pubmed_value = normalised.get("pubmed_id")
    pubmed_int = _safe_int(pubmed_value)
    if pubmed_int is not None:
        normalised["document_pubmed_id"] = pubmed_int
        normalised["chembl_pmid"] = pubmed_int
    else:
        normalised.setdefault("document_pubmed_id", None)
        normalised.setdefault("chembl_pmid", None)

    classification = normalised.get("classification")
    if classification is not None:
        normalised["document_classification"] = classification

    experimental = normalised.get("is_experimental_doc")
    if experimental is not None:
        normalised["original_experimental_document"] = coerce_optional_bool(experimental)

    contains_links = normalised.get("document_contains_external_links")
    if contains_links is not None:
        normalised["referenses_on_previous_experiments"] = coerce_optional_bool(contains_links)

    if "title" in normalised and normalised.get("title") is not None:
        normalised["_original_title"] = normalised["title"]

    for old_col, new_col in {
        "title": "chembl_title",
        "journal": "chembl_journal",
        "year": "chembl_year",
        "authors": "chembl_authors",
        "abstract": "chembl_abstract",
        "volume": "chembl_volume",
        "issue": "chembl_issue",
    }.items():
        if old_col in normalised:
            value = normalised.get(old_col)
            normalised[new_col] = (
                registry.normalize("string", value) if value is not None else None
            )
            if old_col != new_col:
                normalised.pop(old_col, None)

    if "doi" in normalised:
        normalised["chembl_doi"] = normalised.get("doi")

    for column in ("document_chembl_id", "doi", "pmid", "pubmed_id"):
        value = normalised.get(column)
        if value is not None:
            normalised[column] = registry.normalize("identifier", value)

    normalised["chembl_doc_type"] = "journal-article"

    return normalised


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass

    text = str(value).strip()
    if not text or not text.isdigit():
        return None

    try:
        return int(text)
    except ValueError:
        return None


__all__ = ["normalize_document_frame"]
