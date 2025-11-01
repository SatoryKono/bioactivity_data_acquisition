"""Property-based tests for chemistry-related normalizers."""

from __future__ import annotations

import json

import pytest

pytest.importorskip("hypothesis")

from hypothesis import given, settings
from hypothesis import strategies as st

from bioetl.normalizers.chemistry import (
    ChemistryNormalizer,
    ChemistryStringNormalizer,
    ChemistryUnitsNormalizer,
    NonNegativeFloatNormalizer,
)

pytestmark = pytest.mark.property

_chemistry_normalizer = ChemistryNormalizer()
_string_normalizer = ChemistryStringNormalizer()
_units_normalizer = ChemistryUnitsNormalizer()
_non_negative_float_normalizer = NonNegativeFloatNormalizer()


def _smiles_like_strings() -> st.SearchStrategy[str]:
    """Generate SMILES-like strings with arbitrary whitespace noise."""

    alphabet = list("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-=@#$()%[]/.\\ \t\n")
    return st.text(alphabet=alphabet, max_size=60)


def _inchi_like_strings() -> st.SearchStrategy[str]:
    """Generate InChI strings with optional whitespace padding."""

    whitespace = st.text(alphabet=" \t", max_size=3)
    core = st.text(
        alphabet=st.characters(whitelist_categories=("Lu", "Ll", "Nd", "So", "Sc")),
        max_size=60,
    )
    return st.builds(lambda prefix, body, suffix: f"{prefix}InChI=1S/{body}{suffix}", whitespace, core, whitespace)


def _numeric_values() -> st.SearchStrategy[object]:
    """Return inputs for numeric normalisation in the chemistry domain."""

    return st.one_of(
        st.floats(allow_infinity=False),
        st.integers(),
        st.from_regex(r"\s*[+-]?\d{1,6}(?:\.\d{1,6})?\s*", fullmatch=True),
        st.text(max_size=20),
        st.none(),
    )


@given(_smiles_like_strings())
@settings(max_examples=200, deadline=None)
def test_chemistry_normalizer_is_idempotent_for_smiles(value: str) -> None:
    """SMILES normalization should produce stable canonical strings."""

    result = _chemistry_normalizer.normalize(value)
    if result is None:
        return

    assert _chemistry_normalizer.normalize(result) == result
    assert result == result.strip()
    assert "  " not in result


@given(_inchi_like_strings())
@settings(max_examples=200, deadline=None)
def test_chemistry_normalizer_preserves_inchi_prefix(value: str) -> None:
    """Valid InChI strings must be preserved and start with ``InChI=``."""

    result = _chemistry_normalizer.normalize(value)
    if result is None:
        return

    assert result.startswith("InChI=")
    assert result == result.strip()
    assert _chemistry_normalizer.validate(result)


@given(_smiles_like_strings())
@settings(max_examples=200, deadline=None)
def test_chemistry_string_normalizer_enforces_casing(value: str) -> None:
    """Uppercase mode should normalise to uppercase without extra spaces."""

    result = _string_normalizer.normalize(value, uppercase=True)
    if result is None:
        return

    assert result == result.upper()
    assert result == result.strip()
    assert "  " not in result


@given(_smiles_like_strings())
@settings(max_examples=200, deadline=None)
def test_chemistry_units_normalizer_is_idempotent(value: str) -> None:
    """Units normalizer should produce idempotent canonical strings."""

    result = _units_normalizer.normalize(value)
    if result is None:
        return

    assert _units_normalizer.normalize(result) == result
    assert result == result.strip()


@given(_numeric_values())
@settings(max_examples=200, deadline=None)
def test_non_negative_float_normalizer_rejects_negative(value: object) -> None:
    """Non-negative float normalizer should drop negative values while preserving sign."""

    result = _non_negative_float_normalizer.normalize(value)
    if result is None:
        return

    assert result >= 0
    assert isinstance(result, float)
    assert json.dumps(result) is not None
