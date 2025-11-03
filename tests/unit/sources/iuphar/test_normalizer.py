"""IUPHAR normalizer helper tests."""

from __future__ import annotations

from bioetl.sources.iuphar.normalizer import (
    normalize_gene_symbol,
    normalize_target_name,
    unique_preserving_order,
)


def test_normalize_target_name_strips_special_characters() -> None:
    """Target names should be lower-cased and stripped of punctuation."""

    assert normalize_target_name("A2A Receptor") == "a2areceptor"
    assert normalize_target_name(None) == ""


def test_normalize_gene_symbol_preserves_case() -> None:
    """Gene symbols keep their original casing aside from trimming."""

    assert normalize_gene_symbol("  PtgIr  ") == "PtgIr"
    assert normalize_gene_symbol(None) == ""


def test_unique_preserving_order_filters_duplicates_case_insensitively() -> None:
    """Duplicate values are removed while preserving insertion order."""

    values = ["GPCR", "gpcr", "Ion Channel", "ION CHANNEL"]
    assert unique_preserving_order(values) == ["GPCR", "Ion Channel"]
