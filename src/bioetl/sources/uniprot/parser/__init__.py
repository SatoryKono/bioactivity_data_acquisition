"""UniProt parsing utilities."""

from .entry_parser import parse_entry_response, parse_search_response
from .idmapping_parser import parse_idmapping_results, parse_idmapping_status
from .extractors import (
    _extract_gene_primary,
    _extract_gene_synonyms,
    _extract_secondary,
    _extract_protein_name,
    _extract_sequence_length,
    _extract_taxonomy_id,
    _extract_organism,
    _extract_lineage,
    parse_isoforms,
)

__all__ = [
    "parse_entry_response",
    "parse_search_response",
    "parse_idmapping_results",
    "parse_idmapping_status",
    "_extract_gene_primary",
    "_extract_gene_synonyms",
    "_extract_secondary",
    "_extract_protein_name",
    "_extract_sequence_length",
    "_extract_taxonomy_id",
    "_extract_organism",
    "_extract_lineage",
    "parse_isoforms",
]
