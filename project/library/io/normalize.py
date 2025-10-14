"""Normalization helpers for pipeline inputs, outputs, and API payloads."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any

import pandas as pd

QUERY_COLUMNS = ["document_chembl_id", "doi", "pmid"]
PUBLICATION_COLUMNS = [
    "chembl.document_chembl_id",
    "doi_key",
    "pmid",
    "chembl.title",
    "chembl.doi",
    "chembl.pmid",
    "chembl.journal",
    "chembl.year",
    "pubmed.title",
    "pubmed.journal",
    "pubmed.pub_date",
    "pubmed.doi",
    "semscholar.title",
    "semscholar.year",
    "semscholar.paper_id",
    "crossref.title",
    "crossref.issued",
    "crossref.publisher",
    "crossref.type",
    "openalex.title",
    "openalex.publication_year",
    "openalex.doi",
]


def to_lc_stripped(value: Any) -> str | None:
    """Return a lowercase, stripped string when ``value`` is truthy."""

    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()
    return text.lower() or None


def coerce_text(value: Any, *, lowercase: bool = False) -> str | None:
    """Convert arbitrary values into clean text."""

    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
    else:
        text = str(value).strip()
    if not text:
        return None
    return text.lower() if lowercase else text


def normalise_doi(value: Any) -> str | None:
    """Normalise DOI strings by stripping prefixes and lowercasing."""

    text = coerce_text(value, lowercase=True)
    if not text:
        return None
    prefixes = ("https://doi.org/", "http://doi.org/", "doi:")
    for prefix in prefixes:
        if text.startswith(prefix):
            text = text[len(prefix) :]
            break
    return text.strip()


def normalize_query_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with canonical column names and whitespace trimmed."""

    normalized = df.copy()
    normalized.columns = [column.strip().lower() for column in normalized.columns]
    for column in QUERY_COLUMNS:
        if column in normalized.columns:
            normalized[column] = normalized[column].map(coerce_text)
    return normalized.loc[:, [column for column in QUERY_COLUMNS if column in normalized.columns]]


def normalize_publication_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Deterministically order publication columns and sort the frame."""

    normalized = ensure_columns(df, PUBLICATION_COLUMNS)
    normalized = normalized[PUBLICATION_COLUMNS]
    if "chembl.year" in normalized.columns:
        normalized["chembl.year"] = pd.to_numeric(normalized["chembl.year"], errors="coerce")
    if "pubmed.pub_date" in normalized.columns:
        normalized["pubmed.pub_date"] = normalized["pubmed.pub_date"].map(_parse_date_to_str)
    if "openalex.publication_year" in normalized.columns:
        normalized["openalex.publication_year"] = pd.to_numeric(
            normalized["openalex.publication_year"], errors="coerce"
        )
    normalized.sort_values(
        by=["chembl.document_chembl_id", "doi_key", "pmid"],
        inplace=True,
        kind="mergesort",
        na_position="last",
    )
    normalized.reset_index(drop=True, inplace=True)
    return normalized


def ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """Ensure that all columns exist, filling missing ones with ``pd.NA``."""

    enriched = df.copy()
    for column in columns:
        if column not in enriched.columns:
            enriched[column] = pd.NA
    return enriched


def parse_chembl_document(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Extract relevant fields from a ChEMBL document payload."""

    doi = normalise_doi(payload.get("doi"))
    pmid = coerce_text(payload.get("pubmed_id"))
    return {
        "document_chembl_id": coerce_text(payload.get("document_chembl_id")),
        "title": coerce_text(payload.get("title")),
        "doi": doi,
        "pmid": pmid,
        "journal": coerce_text(payload.get("journal")),
        "year": coerce_text(payload.get("year")) or coerce_text(payload.get("publication_year")),
    }


def parse_pubmed_summary(pmid: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    article_ids = payload.get("articleids", [])
    doi_value = None
    for item in article_ids:
        if item.get("idtype", "").lower() == "doi":
            doi_value = item.get("value")
            break
    return {
        "pmid": coerce_text(pmid),
        "title": coerce_text(payload.get("title")),
        "journal": coerce_text(payload.get("fulljournalname")),
        "pub_date": coerce_text(payload.get("pubdate")),
        "doi": normalise_doi(doi_value),
    }


def parse_semscholar_paper(payload: Mapping[str, Any]) -> dict[str, Any]:
    external_ids = payload.get("externalIds", {})
    pmid = coerce_text(external_ids.get("PMID"))
    doi = normalise_doi(external_ids.get("DOI"))
    year = payload.get("year") or payload.get("publicationYear")
    return {
        "paper_id": coerce_text(payload.get("paperId")),
        "title": coerce_text(payload.get("title")),
        "pmid": pmid,
        "doi": doi,
        "year": coerce_text(year),
    }


def parse_crossref_work(payload: Mapping[str, Any]) -> dict[str, Any]:
    message = payload.get("message", payload)
    title = message.get("title") or [None]
    return {
        "title": coerce_text(title[0] if isinstance(title, list) else title),
        "issued": coerce_text(_safe_join_date_parts(message.get("issued", {}).get("date-parts"))),
        "publisher": coerce_text(message.get("publisher")),
        "type": coerce_text(message.get("type")),
        "doi": normalise_doi(message.get("DOI")),
    }


def parse_openalex_work(payload: Mapping[str, Any]) -> dict[str, Any]:
    ids = payload.get("ids", {}) if isinstance(payload, Mapping) else {}
    doi = payload.get("doi") or ids.get("doi")
    if doi and isinstance(doi, str) and doi.startswith("https://doi.org/"):
        doi = doi.replace("https://doi.org/", "")
    pmid = ids.get("pmid") or payload.get("pmid")
    if pmid and isinstance(pmid, str) and "/" in pmid:
        pmid = pmid.rstrip("/").split("/")[-1]
    publication_year = payload.get("publication_year") or payload.get("publicationYear")
    result = {
        "title": coerce_text(payload.get("title") or payload.get("display_name")),
        "publication_year": coerce_text(publication_year),
        "doi": normalise_doi(doi),
        "pmid": coerce_text(pmid),
    }
    return result


def _safe_join_date_parts(parts: Any) -> str | None:
    if not parts:
        return None
    if isinstance(parts, list):
        first = parts[0]
        if isinstance(first, list):
            first = first[:3]
        if isinstance(first, list):
            values = [str(value) for value in first if value is not None]
            return "-".join(values)
    return None


def _parse_date_to_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text)
            return parsed.date().isoformat()
        except ValueError:
            return text
    return str(value)
