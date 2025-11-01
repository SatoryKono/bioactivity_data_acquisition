"""Normalization utilities for the ChEMBL activity normalizer."""

from __future__ import annotations

from bioetl.sources.chembl.activity.normalizer import ActivityNormalizer


def test_derive_is_citation_detects_document_links() -> None:
    """Document identifiers should flag the record as a citation."""

    normalizer = ActivityNormalizer()
    properties: list[dict[str, object]] = []

    assert normalizer.derive_is_citation("CHEMBL-DOC", properties) is True
    assert normalizer.derive_is_citation(None, properties) is False


def test_derive_exact_and_rounded_citations_from_properties() -> None:
    """Citation helpers should honour structured property hints."""

    normalizer = ActivityNormalizer()
    props = [
        {"name": "Exact Data Citation", "value": True},
        {"name": "Rounded Data Citation", "value": 1},
    ]

    assert normalizer.derive_exact_data_citation(None, props) is True
    assert normalizer.derive_rounded_data_citation(None, props) is True


def test_derive_high_citation_rate_threshold() -> None:
    """High citation rate helper should interpret numeric properties."""

    normalizer = ActivityNormalizer()
    props = [{"name": "Citation Count", "value": 75}]
    assert normalizer.derive_high_citation_rate(props) is True

    low_props = [{"name": "Citation Count", "value": 10}]
    assert normalizer.derive_high_citation_rate(low_props) is False


def test_normalize_activity_type_collapses_aliases() -> None:
    """Common synonyms should be transformed into canonical ChEMBL labels."""

    normalizer = ActivityNormalizer()

    assert normalizer.normalize_activity_type("ic 50") == "IC50"
    assert normalizer.normalize_activity_type("pKi") == "pKi"
    assert normalizer.normalize_activity_type("Residual_Activity") == "Residual Activity"
