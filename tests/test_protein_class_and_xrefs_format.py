from __future__ import annotations

import pytest

from src.library.pipelines.target.chembl_target import (
    format_cross_references,
    format_protein_classifications,
)


def test_format_protein_classifications_basic_levels() -> None:
    raw = {
        "protein_classification": [
            {"l1": "Enzyme", "l2": "Kinase", "l3": "Tyrosine kinase"},
            {"l1": "Enzyme", "l2": "Kinase"},
        ]
    }
    result = format_protein_classifications(raw)
    assert result.split("|") == [
        "Enzyme>Kinase",
        "Enzyme>Kinase>Tyrosine kinase",
    ]


def test_format_protein_classifications_alt_keys_and_dedup() -> None:
    raw = {
        "protein_classification": [
            {"level1": "Receptor", "level2": "GPCR", "level3": "Class A"},
            {"l1": "Receptor", "l2": "GPCR", "l3": "Class A"},  # duplicate via alt keys
        ]
    }
    result = format_protein_classifications(raw)
    assert result == "Receptor>GPCR>Class A"


def test_format_protein_classifications_direct_list() -> None:
    raw = [
        {"l1": "Transporter", "l2": "Ion channel"},
    ]
    result = format_protein_classifications(raw)
    assert result == "Transporter>Ion channel"


def test_format_protein_classifications_unknown_shape() -> None:
    assert format_protein_classifications(None) == ""
    assert format_protein_classifications({}) == ""
    assert format_protein_classifications({"protein_classification": [{}]}) == ""


def test_format_cross_references_basic() -> None:
    raw = {
        "cross_reference": [
            {"xref_src_db": "UniProt", "xref_id": "P00533"},
            {"xref_src_db": "HGNC", "xref_id": "1234"},
        ]
    }
    result = format_cross_references(raw)
    # Sorted by SRC then ID; uppercased SRC
    assert result == "HGNC:1234|UNIPROT:P00533"


def test_format_cross_references_direct_list_and_dedup() -> None:
    raw = [
        {"xref_src_db": "UniProt", "xref_id": "Q9H0H5"},
        {"src": "uniprot", "id": "Q9H0H5"},  # duplicate via alt keys
    ]
    result = format_cross_references(raw)
    assert result == "UNIPROT:Q9H0H5"


def test_format_cross_references_unknown_shape() -> None:
    assert format_cross_references(None) == ""
    assert format_cross_references({}) == ""
    assert format_cross_references({"cross_reference": [{}]}) == ""


