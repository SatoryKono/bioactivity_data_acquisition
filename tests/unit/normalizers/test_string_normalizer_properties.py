"""Property-based tests for :mod:`bioetl.normalizers.string`."""

from __future__ import annotations

import string

from hypothesis import given
from hypothesis import strategies as st

from bioetl.normalizers.string import StringNormalizer

normalizer = StringNormalizer()


@given(st.text())
def test_normalize_is_idempotent(value: str) -> None:
    """Normalizing an already-normalized string should be a no-op."""

    result = normalizer.normalize(value)
    if result is None:
        assert normalizer.normalize(result) is None
    else:
        assert normalizer.normalize(result) == result


@given(
    st.lists(
        st.text(alphabet=string.ascii_letters + string.digits, min_size=1, max_size=8),
        min_size=2,
        max_size=6,
    )
)
def test_whitespace_sequences_collapse_to_single_space(tokens: list[str]) -> None:
    """Multiple whitespace runs should be collapsed into single spaces."""

    raw = "\t\n  ".join(tokens)
    result = normalizer.normalize(raw)
    assert result is not None
    expected = " ".join(tokens)
    assert result == expected
    assert "  " not in result


@given(st.sampled_from([None, 1, 2.5, object()]))
def test_non_string_inputs_are_rejected(value: object | None) -> None:
    """Non-string payloads should return ``None`` to signal invalid input."""

    assert normalizer.normalize(value) is None
