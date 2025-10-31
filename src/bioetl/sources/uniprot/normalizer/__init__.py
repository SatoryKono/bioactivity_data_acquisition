"""UniProt enrichment normalization helpers."""

from ..normalizer_service import UniProtEnrichmentResult, UniProtNormalizer
from .normalizer import apply_enrichment, normalize_entry_to_dataframe

__all__ = [
    "apply_enrichment",
    "normalize_entry_to_dataframe",
    "UniProtNormalizer",
    "UniProtEnrichmentResult",
]
