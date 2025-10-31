"""Tests for ChEMBL parsing helpers."""

from __future__ import annotations

from bioetl.sources.chembl.activity.pipeline import _normalize_output_value


def test_normalize_output_value_flattens_iterables() -> None:
    """Lists and tuples should be joined into deterministic strings."""

    values = ["Alpha", None, "Beta"]
    assert _normalize_output_value(values) == "Alpha; Beta"
    assert _normalize_output_value(()) is None


def test_normalize_output_value_returns_scalars_as_is() -> None:
    """Scalar values are returned untouched."""

    assert _normalize_output_value("text") == "text"
    assert _normalize_output_value(123) == 123
