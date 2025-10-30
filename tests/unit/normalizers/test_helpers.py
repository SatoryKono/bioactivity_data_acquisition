"""Tests for normalizer helper utilities."""

import math

import pytest

from bioetl.normalizers.helpers import _is_na, coerce_bool, is_na


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        "   ",
        "NA",
        "n/a",
        " None ",
        "NuLl",
        "nan",
        math.nan,
    ],
)
def test_is_na_positive_cases(value):
    """Values considered missing should return True for NA checks."""

    assert _is_na(value) is True
    assert is_na(value) is True


@pytest.mark.parametrize(
    "value",
    [
        0,
        False,
        [],
        {},
        "false",
        "0",
        "  value  ",
        3.14,
    ],
)
def test_is_na_negative_cases(value):
    """Non-missing values should not be flagged as NA."""

    assert _is_na(value) is False
    assert is_na(value) is False


@pytest.mark.parametrize(
    "value,expected",
    [
        (True, True),
        (False, False),
        (1, True),
        (0, False),
        (2.5, True),
        (0.0, False),
        ("yes", True),
        ("NO", False),
    ],
)
def test_coerce_bool_standard_cases(value, expected):
    """Common truthy/falsey values are coerced consistently."""

    assert coerce_bool(value) is expected


@pytest.mark.parametrize(
    "value",
    [None, "", " n/a ", float("nan")],
)
def test_coerce_bool_na_handling(value):
    """NA values return None by default and honour fallback defaults."""

    assert coerce_bool(value) is None
    assert coerce_bool(value, default=True, allow_na=False) is True
    assert coerce_bool(value, default=False, allow_na=False) is False


@pytest.mark.parametrize(
    "value,default",
    [("maybe", True), ([], False), ({}, True)],
)
def test_coerce_bool_unrecognised_values_return_default(value, default):
    """Values without explicit semantics use the provided default."""

    assert coerce_bool(value, default=default) is default

