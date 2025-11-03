"""Property-based tests for numeric and boolean normalization."""

from __future__ import annotations

import math

import pytest

pytest.importorskip("hypothesis")

from hypothesis import given, settings
from hypothesis import strategies as st

from bioetl.normalizers.numeric import BooleanNormalizer, NumericNormalizer

pytestmark = pytest.mark.property

_numeric_normalizer = NumericNormalizer()
_boolean_normalizer = BooleanNormalizer()


def _numeric_like_strategy() -> st.SearchStrategy[object]:
    """Return diverse inputs that exercise numeric coercion paths."""

    finite_or_nan_floats = st.floats(allow_infinity=False)
    integer_strings = st.from_regex(r"\s*[+-]?\d{1,8}\s*", fullmatch=True)
    decimal_strings = st.from_regex(r"\s*[+-]?\d{1,4}\.\d{1,6}\s*", fullmatch=True)
    arbitrary_text = st.text(max_size=20)
    return st.one_of(
        finite_or_nan_floats,
        st.integers(),
        integer_strings,
        decimal_strings,
        arbitrary_text,
        st.booleans(),
        st.none(),
    )


def _int_like_strategy() -> st.SearchStrategy[object]:
    """Generate inputs that may coerce to integers."""

    return st.one_of(
        st.integers(),
        st.from_regex(r"\s*[+-]?\d{1,12}\s*", fullmatch=True),
        st.text(max_size=12),
        st.none(),
    )


@given(_numeric_like_strategy())
@settings(max_examples=200, deadline=None)
def test_normalize_float_is_idempotent(value: object) -> None:
    """Float normalization should be idempotent for stable results."""

    first = _numeric_normalizer.normalize_float(value)
    second = (
        _numeric_normalizer.normalize_float(first)
        if first is not None
        else None
    )

    assert second == first
    if first is not None:
        assert isinstance(first, float)
        assert not math.isnan(first)


@given(_numeric_like_strategy())
@settings(max_examples=200, deadline=None)
def test_validate_matches_normalize(value: object) -> None:
    """Validation failures must correspond to non-normalisable inputs."""

    validated = _numeric_normalizer.validate(value)
    result = _numeric_normalizer.normalize(value)

    if not validated:
        assert result is None
    if result is not None:
        assert _numeric_normalizer.validate(result)
        assert isinstance(result, float)
        assert not math.isnan(result)


@given(_int_like_strategy())
@settings(max_examples=200, deadline=None)
def test_normalize_int_returns_canonical_integer(value: object) -> None:
    """Integer normalization should yield stable canonical integers."""

    first = _numeric_normalizer.normalize_int(value)
    if first is None:
        assert first is None
        return

    assert isinstance(first, int)
    second = _numeric_normalizer.normalize_int(first)
    assert second == first


@given(
    st.sampled_from([None, "", " ", "na", "NA", "n/a", float("nan")]),
    st.booleans(),
)
@settings(max_examples=200, deadline=None)
def test_normalize_bool_respects_default_for_missing(value: object, default: bool) -> None:
    """Boolean coercion should fall back to the provided default for NA values."""

    result = _numeric_normalizer.normalize_bool(value, default=default)
    assert result is default


@given(_numeric_like_strategy())
@settings(max_examples=200, deadline=None)
def test_boolean_normalizer_is_idempotent(value: object) -> None:
    """Boolean normalizer should be idempotent on recognised values."""

    first = _boolean_normalizer.normalize(value)
    second = (
        _boolean_normalizer.normalize(first)
        if first is not None
        else None
    )

    assert second == first
    if first is not None:
        assert isinstance(first, bool)
        assert _boolean_normalizer.validate(first)
