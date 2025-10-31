"""Normalization utilities for ChEMBL activity pipeline."""

from __future__ import annotations

from bioetl.sources.chembl.activity.pipeline import (
    _derive_exact_data_citation,
    _derive_high_citation_rate,
    _derive_is_citation,
    _derive_rounded_data_citation,
)


def test_derive_is_citation_detects_document_links() -> None:
    """Document identifiers should flag the record as a citation."""

    properties: list[dict[str, object]] = []
    assert _derive_is_citation("CHEMBL-DOC", properties) is True
    assert _derive_is_citation(None, properties) is False


def test_derive_exact_and_rounded_citations_from_properties() -> None:
    """Citation helpers should honour structured property hints."""

    props = [
        {"name": "Exact Data Citation", "value": True},
        {"name": "Rounded Data Citation", "value": 1},
    ]

    assert _derive_exact_data_citation(None, props) is True
    assert _derive_rounded_data_citation(None, props) is True


def test_derive_high_citation_rate_threshold() -> None:
    """High citation rate helper should interpret numeric properties."""

    props = [{"name": "Citation Count", "value": 75}]
    assert _derive_high_citation_rate(props) is True

    low_props = [{"name": "Citation Count", "value": 10}]
    assert _derive_high_citation_rate(low_props) is False
