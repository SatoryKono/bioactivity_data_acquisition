"""Property-based tests for string normalization."""

from __future__ import annotations

from hypothesis import given, strategies as st

from bioetl.normalizers.string import StringNormalizer


normalizer = StringNormalizer()


@given(st.text())
def test_string_normalizer_is_idempotent(value: str) -> None:
    """Normalizing a string multiple times should not change the result."""

    once = normalizer.normalize(value)
    twice = normalizer.normalize(once) if once is not None else None

    assert twice == once
    if once is not None:
        assert once == once.strip()
        assert "  " not in once


_WHITESPACE_CHARS = [" ", "\t", "\n", "\r", "\f", "\v"]


@given(st.text(alphabet=st.sampled_from(_WHITESPACE_CHARS), min_size=1))
def test_string_normalizer_returns_none_for_blank_strings(blank: str) -> None:
    """Strings that collapse to whitespace should normalise to ``None``."""

    assert normalizer.normalize(blank) is None
