"""UniProt data access helpers and pipeline exports."""

from .normalizer import UniProtEnrichmentResult, UniProtNormalizer
from .pipeline import UniProtPipeline

__all__ = ["UniProtNormalizer", "UniProtEnrichmentResult", "UniProtPipeline"]
