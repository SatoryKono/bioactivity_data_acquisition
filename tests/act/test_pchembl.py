from __future__ import annotations

import pytest

from library.postprocess.units import normalize_unit, pchembl_from_value, to_nM


def _compute(value: float, unit: str, relation: str) -> float | None:
    normalized = normalize_unit(unit)
    value_nm = to_nM(value, normalized)
    if relation in {"=", "≤", "<="}:
        return pchembl_from_value(value_nm)
    return None


def test_pchembl_for_equality_relation() -> None:
    result = _compute(10.0, "nM", "=")
    assert result == pytest.approx(8.0)


def test_pchembl_for_less_equal_relation() -> None:
    result = _compute(100.0, "nM", "<=")
    assert result == pytest.approx(7.0)


def test_pchembl_is_null_for_other_relations() -> None:
    assert _compute(10.0, "nM", "<") is None
    assert _compute(10.0, "nM", ">") is None
