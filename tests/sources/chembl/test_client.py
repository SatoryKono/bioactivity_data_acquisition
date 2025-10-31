"""Tests for helper utilities in the ChEMBL activity normalizer."""

from __future__ import annotations

from bioetl.sources.chembl.activity.normalizer import ActivityNormalizer


def test_normalize_int_scalar_handles_blank_values() -> None:
    """Empty strings or invalid values should return ``None``."""

    assert ActivityNormalizer.normalize_int_scalar("  ") is None
    assert ActivityNormalizer.normalize_int_scalar("42") == 42
    assert ActivityNormalizer.normalize_int_scalar(None) is None


def test_derive_compound_key_requires_all_components() -> None:
    """Compound keys are only produced when all identifiers are present."""

    normalizer = ActivityNormalizer()

    assert normalizer.derive_compound_key("CHEMBL1", "IC50", "CHEMBL2") == "CHEMBL1|IC50|CHEMBL2"
    assert normalizer.derive_compound_key("CHEMBL1", None, "CHEMBL2") is None
