from __future__ import annotations

import math

import pytest

from library.postprocess.units import (
    PChEMBLComputationError,
    UnitNormalizationError,
    normalize_unit,
    pchembl_from_value,
    to_nM,
)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("µM", "uM"),
        ("μM", "uM"),
        (" nM ", "nM"),
        ("PM", "pM"),
        ("mM", "mM"),
    ],
)
def test_normalize_unit_variants(raw: str, expected: str) -> None:
    assert normalize_unit(raw) == expected


def test_normalize_unit_rejects_unknown() -> None:
    with pytest.raises(UnitNormalizationError) as exc:
        normalize_unit("kg")
    assert exc.value.code == "unit_unknown"


@pytest.mark.parametrize(
    "value,unit,expected",
    [
        (1.0, "pM", 1e-3),
        (1.0, "nM", 1.0),
        (1.0, "uM", 1e3),
        (2.5, "mM", 2.5e6),
        (3.5, "M", 3.5e9),
    ],
)
def test_to_nanomolar(value: float, unit: str, expected: float) -> None:
    assert math.isclose(to_nM(value, unit), expected)


def test_pchembl_rounding_half_even() -> None:
    value_nM = float(10 ** (9 - 6.125))
    result = pchembl_from_value(value_nM)
    assert result == pytest.approx(6.12)


def test_pchembl_out_of_range() -> None:
    with pytest.raises(PChEMBLComputationError) as exc:
        pchembl_from_value(1e-12)
    assert exc.value.code == "pchembl_out_of_range"
