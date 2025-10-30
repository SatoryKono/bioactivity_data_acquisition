"""Unit tests for chemistry normalizers and registry integration."""

import math

import pytest

from bioetl.normalizers import registry
from bioetl.normalizers.chemistry import (
    BaoIdNormalizer,
    ChemblIdNormalizer,
    ChemistryRelationNormalizer,
    ChemistryStringNormalizer,
    ChemistryUnitsNormalizer,
    LigandEfficiencyNormalizer,
    NonNegativeFloatNormalizer,
    TargetOrganismNormalizer,
)


@pytest.mark.parametrize(
    "value,kwargs,expected",
    [
        ("  Ki  ", {"uppercase": True}, "KI"),
        ("beta arrestin", {"title_case": True}, "Beta Arrestin"),
        ("  mixed   case  ", {}, "mixed case"),
        ("N/A", {}, None),
        (None, {}, None),
        ("truncate", {"max_length": 4}, "trun"),
    ],
)
def test_chemistry_string_normalizer(value, kwargs, expected):
    normalizer = ChemistryStringNormalizer()
    assert normalizer.normalize(value, **kwargs) == expected


def test_chemistry_relation_normalizer_variants():
    normalizer = ChemistryRelationNormalizer()
    assert normalizer.normalize("==") == "="
    assert normalizer.normalize("≤") == "<="
    assert normalizer.normalize("unknown") == "unknown"
    assert normalizer.normalize(None) == "="
    assert normalizer.normalize("", default=">") == ">"


def test_chemistry_units_normalizer_synonyms():
    normalizer = ChemistryUnitsNormalizer()
    assert normalizer.normalize("nm") == "nM"
    assert normalizer.normalize("μm") == "µM"
    assert normalizer.normalize("  ug/mL  ") == "µg/mL"
    assert normalizer.normalize("", default="nM") == "nM"
    assert normalizer.normalize(None) is None
    assert normalizer.normalize("kg") == "kg"


def test_chembl_id_normalizer_uppercase():
    normalizer = ChemblIdNormalizer()
    assert normalizer.normalize("chembl123") == "CHEMBL123"
    assert normalizer.normalize(None) is None


def test_bao_id_normalizer_patterns():
    normalizer = BaoIdNormalizer()
    assert normalizer.normalize("bao:0000219") == "BAO_0000219"
    assert normalizer.normalize("BAO_0000219") == "BAO_0000219"
    assert normalizer.normalize("  bao_term  ") == "BAO_TERM"


def test_target_organism_normalizer():
    normalizer = TargetOrganismNormalizer()
    assert normalizer.normalize("homo sapiens") == "Homo Sapiens"
    assert normalizer.normalize("E. coli") == "E. Coli"


def test_non_negative_float_normalizer():
    normalizer = NonNegativeFloatNormalizer()
    assert normalizer.normalize(5.5) == 5.5
    assert normalizer.normalize(-1.0) is None
    assert normalizer.normalize(math.nan) is None


def test_ligand_efficiency_normalizer():
    normalizer = LigandEfficiencyNormalizer()
    payload = {"bei": "1.2", "sei": 3, "le": None, "lle": "4.5"}
    assert normalizer.normalize(payload) == (1.2, 3.0, None, 4.5)
    assert normalizer.normalize(None) == (None, None, None, None)


@pytest.mark.parametrize(
    "name,value,kwargs,expected",
    [
        ("chemistry.string", "  example  ", {}, "example"),
        ("chemistry.relation", "≤", {}, "<="),
        ("chemistry.units", "μg/ml", {}, "µg/mL"),
        ("chemistry.non_negative_float", -5, {}, None),
    ],
)
def test_registry_normalize_dispatch(name, value, kwargs, expected):
    assert registry.normalize(name, value, **kwargs) == expected
