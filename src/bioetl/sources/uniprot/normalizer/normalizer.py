"""Normalization helpers for UniProt enrichment."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd

from bioetl.normalizers.identifier import IdentifierNormalizer
from bioetl.sources.uniprot.parser.extractors import (
    _extract_gene_primary,
    _extract_gene_synonyms,
    _extract_lineage,
    _extract_organism,
    _extract_protein_name,
    _extract_secondary,
    _extract_sequence_length,
    _extract_taxonomy_id,
)


def _normalize_secondary(
    values: list[str], identifier_normalizer: IdentifierNormalizer | None
) -> list[str]:
    normalized: list[str] = []
    for value in values:
        normalized_value = (
            identifier_normalizer.normalize(value) if identifier_normalizer else None
        )
        normalized.append(normalized_value or value)
    return normalized


def normalize_entry_to_dataframe(
    entry: Mapping[str, Any],
    *,
    identifier_normalizer: IdentifierNormalizer | None = None,
) -> pd.DataFrame:
    """Convert a UniProt entry into a single-row dataframe with normalized fields."""

    normalizer = identifier_normalizer or IdentifierNormalizer()

    canonical = entry.get("primaryAccession") or entry.get("accession")
    canonical_str = str(canonical) if canonical else None
    canonical_normalized = normalizer.normalize(canonical_str) if canonical_str else None

    gene_synonyms = _extract_gene_synonyms(entry)
    secondary_accessions = _extract_secondary(entry)
    secondary_normalized = _normalize_secondary(secondary_accessions, normalizer)
    lineage = _extract_lineage(entry)

    row = {
        "uniprot_canonical_accession": canonical_normalized or canonical_str,
        "uniprot_gene_primary": _extract_gene_primary(entry),
        "uniprot_gene_synonyms": "; ".join(gene_synonyms),
        "uniprot_protein_name": _extract_protein_name(entry),
        "uniprot_sequence_length": _extract_sequence_length(entry),
        "uniprot_taxonomy_id": _extract_taxonomy_id(entry),
        "uniprot_taxonomy_name": _extract_organism(entry),
        "uniprot_lineage": "; ".join(lineage),
        "uniprot_secondary_accessions": "; ".join(secondary_normalized),
    }

    return pd.DataFrame([row]).convert_dtypes()


def apply_enrichment(
    df: pd.DataFrame,
    idx: int,
    entry: Mapping[str, Any],
    *,
    merge_strategy: str,
    identifier_normalizer: IdentifierNormalizer | None = None,
) -> None:
    """Apply normalized enrichment data to ``df`` at ``idx``."""

    normalized = normalize_entry_to_dataframe(
        entry, identifier_normalizer=identifier_normalizer
    ).iloc[0]
    for column, value in normalized.items():
        df.at[idx, column] = value
    df.at[idx, "uniprot_merge_strategy"] = merge_strategy
