"""Property-based tests for column validation helpers."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from bioetl.utils.column_constants import normalise_ignore_suffixes


@given(st.lists(st.one_of(st.text(), st.integers(), st.none(), st.booleans()), max_size=20))
def test_normalise_ignore_suffixes_preserves_first_occurrence_order(values: list[object]) -> None:
    """Normalisation should keep the first occurrence of each non-empty value."""

    result = normalise_ignore_suffixes(values)

    expected: list[str] = []
    for value in values:
        candidate = str(value).strip().lower()
        if candidate and candidate not in expected:
            expected.append(candidate)

    assert list(result) == expected


@given(st.lists(st.one_of(st.text(), st.integers(), st.none(), st.booleans()), max_size=20))
def test_normalise_ignore_suffixes_outputs_unique_lowercase(values: list[object]) -> None:
    """Normalised suffixes must be unique, lower-case and stripped."""

    result = normalise_ignore_suffixes(values)

    assert len(result) == len(set(result))
    for suffix in result:
        assert suffix == suffix.strip()
        assert suffix == suffix.lower()
        assert suffix
