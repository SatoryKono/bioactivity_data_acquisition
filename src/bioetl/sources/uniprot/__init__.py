"""UniProt data access helpers and pipeline exports."""

from .merge.service import UniProtService
from .normalizer_service import UniProtEnrichmentResult, UniProtNormalizer
from .pipeline import UniProtPipeline

__all__ = [
    "UniProtNormalizer",
    "UniProtEnrichmentResult",
    "UniProtPipeline",
    "UniProtService",
]
