"""Unit tests for boolean normalisation utilities."""

import math

import pytest

from bioetl.normalizers.numeric import BooleanNormalizer, NumericNormalizer


@pytest.fixture
def numeric_normalizer() -> NumericNormalizer:
    """Provide a reusable numeric normalizer instance."""

    return NumericNormalizer()


@pytest.fixture
def boolean_normalizer() -> BooleanNormalizer:
    """Provide a reusable boolean normalizer instance."""

    return BooleanNormalizer()


@pytest.mark.parametrize(
    "value,default,expected",
    [
        (None, False, False),
        ("", True, True),
        ("yes", False, True),
        ("NO", True, False),
        (1, False, True),
        (0, True, False),
        (2.3, False, True),
        (math.nan, True, True),
        ("maybe", False, False),
    ],
)
def test_numeric_normalize_bool(value, default, expected, numeric_normalizer):
    """Numeric normalizer should defer to shared coercion rules."""

    result = numeric_normalizer.normalize_bool(value, default=default)
    assert result is expected


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        ("", None),
        (math.nan, None),
        (True, True),
        (False, False),
        (1, True),
        (0, False),
        (2.5, True),
        ("yes", True),
        ("no", False),
        ("maybe", None),
    ],
)
def test_boolean_normalize(value, expected, boolean_normalizer):
    """Boolean normalizer returns ``None`` only for NA/unknown values."""

    assert boolean_normalizer.normalize(value) is expected


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, True),
        ("yes", True),
        ("no", True),
        ("maybe", False),
        ([], False),
    ],
)
def test_boolean_validate(value, expected, boolean_normalizer):
    """Validation mirrors coercion rules and allows NA inputs."""

    assert boolean_normalizer.validate(value) is expected


@pytest.mark.parametrize(
    "value,default,expected",
    [
        (None, False, False),
        (None, True, True),
        ("maybe", True, True),
        ("maybe", False, False),
        ("yes", False, True),
        (0, True, False),
    ],
)
def test_boolean_normalize_with_default(value, default, expected, boolean_normalizer):
    """Normalization with default replaces NA/unknown values with fallback."""

    assert boolean_normalizer.normalize_with_default(value, default=default) is expected
