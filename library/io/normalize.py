"""Utility helpers for normalising textual fields returned by data sources."""
from __future__ import annotations

from typing import Any, Iterable, Mapping

import re


_DOI_PREFIX_RE = re.compile(r"^(?:https?://(?:dx\.)?doi\.org/|doi:)", re.IGNORECASE)
_DOI_ILLEGAL_SUFFIX_RE = re.compile(r"[\.;,]+$")
_WHITESPACE_RE = re.compile(r"\s+")


def to_lc_stripped(value: Any) -> str | None:
    """Return the lower-cased and stripped value or ``None``.

    The helper gracefully accepts any value, converting it to string while making sure
    that empty values are normalised to ``None`` which simplifies downstream
    processing. The transformation is intentionally conservative and keeps the
    content intact apart from removing surrounding whitespace and folding to lower
    case.
    """

    if value is None:
        return None

    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            value = value.decode("utf-8", errors="ignore")

    text = str(value).strip()
    if not text:
        return None
    return text.lower()


def coerce_text(value: Any) -> str | None:
    """Coerce ``value`` to a cleaned textual representation.

    * ``None`` and empty strings are converted to ``None``;
    * sequences are joined by a space to retain most of the content when API
      responses store titles or author names as lists;
    * consecutive whitespace characters are collapsed into a single space.
    """

    if value is None:
        return None

    try:  # pragma: no cover - optional dependency guard
        import pandas as _pd  # type: ignore

        if _pd.isna(value):
            return None
    except Exception:  # noqa: BLE001 - fallback when pandas is unavailable
        pass

    if isinstance(value, str):
        text = value
    elif isinstance(value, bytes):
        try:
            text = value.decode("utf-8")
        except UnicodeDecodeError:
            text = value.decode("utf-8", errors="ignore")
    elif isinstance(value, Iterable) and not isinstance(value, (dict, set)):
        text = " ".join(str(item) for item in value if item is not None)
    else:
        text = str(value)

    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text or None


def normalise_doi(doi: Any) -> str | None:
    """Normalise different DOI representations to a canonical, lower-cased form."""

    raw = coerce_text(doi)
    if raw is None:
        return None

    cleaned = _DOI_PREFIX_RE.sub("", raw)
    cleaned = cleaned.replace(" ", "")
    cleaned = _DOI_ILLEGAL_SUFFIX_RE.sub("", cleaned)
    cleaned = cleaned.lower()

    # Invalid DOIs typically do not contain a slash. Guard against accidental IDs.
    if "/" not in cleaned:
        return None

    return cleaned


def _list_from_response(candidate: Any) -> list[Mapping[str, Any]]:
    if candidate is None:
        return []
    if isinstance(candidate, Mapping):
        return [candidate]
    if isinstance(candidate, list):
        return [item for item in candidate if isinstance(item, Mapping)]
    return []


def parse_chembl_response(response: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Parse the ChEMBL response into a list of normalised document records."""

    documents = []
    if not isinstance(response, Mapping):
        return []

    if "documents" in response:
        documents = _list_from_response(response.get("documents"))
    elif "items" in response:
        documents = _list_from_response(response.get("items"))
    else:
        documents = _list_from_response(response)

    parsed: list[dict[str, Any]] = []
    for doc in documents:
        document_id = coerce_text(
            doc.get("document_chembl_id") or doc.get("chembl_id") or doc.get("id")
        )
        doi = normalise_doi(doc.get("doi") or doc.get("document_doi"))
        pmid = coerce_text(doc.get("pubmed_id") or doc.get("pmid"))
        parsed.append(
            {
                "source": "chembl",
                "document_chembl_id": document_id,
                "doi": coerce_text(doc.get("doi") or doc.get("document_doi")),
                "doi_key": doi,
                "pmid": pmid,
                "title": coerce_text(doc.get("title") or doc.get("document_title")),
            }
        )
    return parsed


def parse_crossref_response(response: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Parse Crossref response payloads.

    The Crossref API usually wraps the payload inside a ``message`` object. Titles
    and authors can be returned as lists; this helper flattens them into strings and
    normalises the DOI key.
    """

    if not isinstance(response, Mapping):
        return []

    payload: Mapping[str, Any]
    if "message" in response and isinstance(response["message"], Mapping):
        payload = response["message"]
    else:
        payload = response

    items = payload.get("items")
    records = _list_from_response(items) if items else [payload]

    parsed: list[dict[str, Any]] = []
    for record in records:
        title = record.get("title")
        if isinstance(title, list):
            title = title[0]
        parsed.append(
            {
                "source": "crossref",
                "doi": coerce_text(record.get("DOI") or record.get("doi")),
                "doi_key": normalise_doi(record.get("DOI") or record.get("doi")),
                "pmid": coerce_text(record.get("PMID") or record.get("pmid")),
                "title": coerce_text(title),
                "issued": coerce_text(record.get("issued")),
            }
        )
    return parsed


def parse_pubmed_response(response: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Parse responses returned by the PubMed E-utilities API."""

    if not isinstance(response, Mapping):
        return []

    result = response.get("result")
    if not isinstance(result, Mapping):
        # sometimes the payload is directly a record mapping
        result = response

    uids = []
    if isinstance(result.get("uids"), list):
        uids = [coerce_text(uid) for uid in result.get("uids")]

    records: list[Mapping[str, Any]] = []
    if uids:
        for uid in uids:
            if uid is None:
                continue
            payload = result.get(uid)
            if isinstance(payload, Mapping):
                records.append(payload)
    else:
        records = _list_from_response(result)

    parsed: list[dict[str, Any]] = []
    for record in records:
        pmid = coerce_text(record.get("uid") or record.get("pmid"))
        parsed.append(
            {
                "source": "pubmed",
                "pmid": pmid,
                "title": coerce_text(record.get("title") or record.get("fulljournalname")),
                "doi": coerce_text(record.get("elocationid") or record.get("doi")),
                "doi_key": normalise_doi(record.get("elocationid") or record.get("doi")),
            }
        )
    return parsed
