"""Data normalizers: string, numeric, chemistry, identifier."""

from bioetl.normalizers.base import BaseNormalizer
from bioetl.normalizers.bibliography import (
    normalize_authors,
    normalize_doi,
    normalize_title,
)
from bioetl.normalizers.chemistry import (
    BaoIdNormalizer,
    ChemblIdNormalizer,
    ChemistryNormalizer,
    ChemistryRelationNormalizer,
    ChemistryStringNormalizer,
    ChemistryUnitsNormalizer,
    LigandEfficiencyNormalizer,
    NonNegativeFloatNormalizer,
    TargetOrganismNormalizer,
)
from bioetl.normalizers.date import DateNormalizer
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
registry.register("chemistry.units", ChemistryUnitsNormalizer())
registry.register("chemistry.chembl_id", ChemblIdNormalizer())
registry.register("chemistry.bao_id", BaoIdNormalizer())
registry.register("chemistry.target_organism", TargetOrganismNormalizer())
registry.register("chemistry.non_negative_float", NonNegativeFloatNormalizer())
registry.register("chemistry.ligand_efficiency", LigandEfficiencyNormalizer())
registry.register("numeric", NumericNormalizer())
registry.register("boolean", BooleanNormalizer())
registry.register("date", DateNormalizer())

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
    "DateNormalizer",
    "NormalizerRegistry",
    "registry",
    "normalize_doi",
    "normalize_title",
    "normalize_authors",
]

