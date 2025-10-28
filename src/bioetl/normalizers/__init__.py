"""Data normalizers: string, numeric, chemistry, identifier."""

from bioetl.normalizers.base import BaseNormalizer
from bioetl.normalizers.chemistry import ChemistryNormalizer
from bioetl.normalizers.identifier import IdentifierNormalizer
from bioetl.normalizers.registry import NormalizerRegistry, registry
from bioetl.normalizers.string import StringNormalizer

# Register default normalizers
registry.register("string", StringNormalizer())
registry.register("identifier", IdentifierNormalizer())
registry.register("chemistry", ChemistryNormalizer())

__all__ = [
    "BaseNormalizer",
    "StringNormalizer",
    "IdentifierNormalizer",
    "ChemistryNormalizer",
    "NormalizerRegistry",
    "registry",
]

