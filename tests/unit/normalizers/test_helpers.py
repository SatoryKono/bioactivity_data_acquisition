"""Tests for normalizer helper utilities."""

import math

import pytest

from bioetl.normalizers.helpers import _is_na, is_na


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

