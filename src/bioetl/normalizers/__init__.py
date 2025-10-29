"""Data normalizers: string, numeric, chemistry, identifier."""

from bioetl.normalizers.base import BaseNormalizer
from bioetl.normalizers.chemistry import (
    BaoIdNormalizer,
    BooleanNormalizer,
    ChemblIdNormalizer,
    ChemistryNormalizer,
    ChemistryStringNormalizer,
    ChemistryUnitNormalizer,
    FloatNormalizer,
    IntNormalizer,
    RelationNormalizer,
)
from bioetl.normalizers.identifier import IdentifierNormalizer
from bioetl.normalizers.registry import NormalizerRegistry, registry
from bioetl.normalizers.string import StringNormalizer

# Register default normalizers
registry.register("string", StringNormalizer())
registry.register("identifier", IdentifierNormalizer())
registry.register("chemistry", ChemistryNormalizer())
registry.register("chemistry.string", ChemistryStringNormalizer())
registry.register("chemistry.string.upper", ChemistryStringNormalizer(uppercase=True))
registry.register("chemistry.string.title", ChemistryStringNormalizer(title_case=True))
registry.register("chemistry.string.max_128", ChemistryStringNormalizer(max_length=128))
registry.register("chemistry.chembl_id", ChemblIdNormalizer())
registry.register("chemistry.chembl_id.strict", ChemblIdNormalizer(strict=True))
registry.register("chemistry.bao_id", BaoIdNormalizer())
registry.register("chemistry.units", ChemistryUnitNormalizer())
registry.register("chemistry.units.default_nm", ChemistryUnitNormalizer(default="nM"))
registry.register("chemistry.relation", RelationNormalizer())
registry.register("chemistry.float", FloatNormalizer())
registry.register("chemistry.int", IntNormalizer())
registry.register("chemistry.bool.false", BooleanNormalizer(default=False))

__all__ = [
    "BaseNormalizer",
    "StringNormalizer",
    "IdentifierNormalizer",
    "ChemistryNormalizer",
    "ChemistryStringNormalizer",
    "ChemblIdNormalizer",
    "BaoIdNormalizer",
    "ChemistryUnitNormalizer",
    "RelationNormalizer",
    "FloatNormalizer",
    "IntNormalizer",
    "BooleanNormalizer",
    "NormalizerRegistry",
    "registry",
]

