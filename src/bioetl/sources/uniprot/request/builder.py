"""Utilities for crafting UniProt REST API request parameters."""

from __future__ import annotations

from collections.abc import Iterable

__all__ = [
    "build_search_query",
    "build_idmapping_payload",
    "build_gene_query",
    "build_ortholog_query",
]


def _normalize_values(values: Iterable[str | int | float | None]) -> list[str]:
    """Coerce arbitrary values into a list of non-empty string tokens."""

    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _escape_term(value: str) -> str:
    """Escape a UniProt query term using double quotes when needed."""

    if not value:
        return value
    escaped = value.replace('"', r'"')
    if any(char.isspace() for char in escaped) or ':' in escaped:
        return f'"{escaped}"'
    return escaped


def build_search_query(accessions: Iterable[str | int | float | None]) -> str:
    """Construct a UniProt search query targeting the provided accessions."""

    tokens = _normalize_values(accessions)
    if not tokens:
        return ""
    terms = [f"(accession:{_escape_term(token)})" for token in tokens]
    return " OR ".join(terms)


def build_gene_query(gene_symbol: str, organism: str | None = None) -> str:
    """Construct a gene-centric UniProt search query."""

    gene = _escape_term(str(gene_symbol).strip())
    if not gene:
        return ""
    query = f"gene_exact:{gene}"
    organism_value = str(organism).strip() if organism is not None else ""
    if organism_value:
        query = f"{query} AND organism_name:{_escape_term(organism_value)}"
    return query


def build_idmapping_payload(
    identifiers: Iterable[str | int | float | None],
    *,
    source: str = "UniProtKB_AC-ID",
    target: str = "UniProtKB",
) -> dict[str, str]:
    """Build the payload for the UniProt ID mapping ``/run`` endpoint."""

    ids = _normalize_values(identifiers)
    return {
        "from": source,
        "to": target,
        "ids": ",".join(ids),
    }


def build_ortholog_query(accession: str) -> str:
    """Create a UniProt ortholog search query for the given accession."""

    accession_value = _escape_term(str(accession).strip())
    if not accession_value:
        return ""
    return (
        "relationship_type:ortholog AND "
        f"(accession:{accession_value} OR xref:{accession_value})"
    )
