"""UniProt enrichment normalization helpers."""

from ..normalizer_service import UniProtNormalizer
from .normalizer import apply_enrichment, normalize_entry_to_dataframe
from .types import UniProtEnrichmentResult

__all__ = [
    "apply_enrichment",
    "normalize_entry_to_dataframe",
    "UniProtNormalizer",
    "UniProtEnrichmentResult",
]
