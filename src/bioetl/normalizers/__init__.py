"""Data normalizers: string, numeric, chemistry, identifier."""

from bioetl.normalizers.base import BaseNormalizer
from bioetl.normalizers.chemistry import (
    BaoIdNormalizer,
    ChemistryNormalizer,
    ChemistryStringNormalizer,
    ChemistryRelationNormalizer,
    ChemistryUnitsNormalizer,
    ChemblIdNormalizer,
    LigandEfficiencyNormalizer,
    NonNegativeFloatNormalizer,
    TargetOrganismNormalizer,
)
from bioetl.normalizers.identifier import IdentifierNormalizer
from bioetl.normalizers.numeric import BooleanNormalizer, NumericNormalizer
from bioetl.normalizers.registry import NormalizerRegistry, registry
from bioetl.normalizers.string import StringNormalizer

# Register default normalizers
registry.register("string", StringNormalizer())
registry.register("identifier", IdentifierNormalizer())
registry.register("chemistry", ChemistryNormalizer())
registry.register("chemistry.string", ChemistryStringNormalizer())
registry.register("chemistry.relation", ChemistryRelationNormalizer())
registry.register("chemistry.chembl_id", ChemblIdNormalizer())
registry.register("chemistry.bao_id", BaoIdNormalizer())
registry.register("chemistry.target_organism", TargetOrganismNormalizer())
registry.register("chemistry.units", ChemistryUnitsNormalizer())
registry.register("chemistry.non_negative_float", NonNegativeFloatNormalizer())
registry.register("chemistry.ligand_efficiency", LigandEfficiencyNormalizer())
registry.register("numeric", NumericNormalizer())
registry.register("boolean", BooleanNormalizer())

__all__ = [
    "BaseNormalizer",
    "StringNormalizer",
    "IdentifierNormalizer",
    "ChemistryNormalizer",
    "ChemistryStringNormalizer",
    "ChemistryRelationNormalizer",
    "ChemistryUnitsNormalizer",
    "ChemblIdNormalizer",
    "BaoIdNormalizer",
    "TargetOrganismNormalizer",
    "NonNegativeFloatNormalizer",
    "LigandEfficiencyNormalizer",
    "NumericNormalizer",
    "BooleanNormalizer",
    "NormalizerRegistry",
    "registry",
]

