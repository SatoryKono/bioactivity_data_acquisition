"""UniProt data access helpers and pipeline exports."""

from .normalizer_service import UniProtNormalizer
from .pipeline import UniProtPipeline
from .normalizer.types import UniProtEnrichmentResult

# Backwards-compatible alias: single source of truth is `UniProtNormalizer`
UniProtService = UniProtNormalizer

__all__ = [
    "UniProtNormalizer",
    "UniProtEnrichmentResult",
    "UniProtPipeline",
    "UniProtService",
]
