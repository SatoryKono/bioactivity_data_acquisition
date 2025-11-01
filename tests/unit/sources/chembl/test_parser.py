"""Tests for ChEMBL activity parser helpers."""

from __future__ import annotations

from bioetl.sources.chembl.activity.normalizer import ActivityNormalizer
from bioetl.sources.chembl.activity.parser import ActivityParser


def test_parser_normalizes_activity_properties_and_citations() -> None:
    """Activity parser should canonicalize properties and derive metadata."""

    parser = ActivityParser(normalizer=ActivityNormalizer(), chembl_release="33")

    record = parser.parse(
        {
            "activity_id": "123",
            "molecule_chembl_id": "CHEMBL1",
            "assay_chembl_id": "CHEMBL2",
            "assay_id": "456",
            "target_chembl_id": "CHEMBL3",
            "document_chembl_id": "CHEMBL4",
            "type": "ic 50",
            "relation": "=",
            "value": "12.5",
            "units": "nanomolar",
            "standard_type": "IC50",
            "standard_relation": "=",
            "standard_value": "12.5",
            "standard_units": None,
            "standard_flag": "1",
            "standard_lower_value": "10",
            "standard_upper_value": "15",
            "activity_properties": [
                {"name": "Exact Data Citation", "value": True},
                {"name": "Citation Count", "value": 51},
            ],
            "data_validity_comment": "Rounded data citation present",
            "target_organism": "Homo sapiens",
            "target_tax_id": "9606",
            "src_id": "2",
            "uo_units": "UO:0000064",
        }
    )

    assert record["activity_id"] == 123
    assert record["assay_id"] == 456
    assert record["compound_key"] == "CHEMBL1|IC50|CHEMBL3"
    assert record["is_citation"] is True
    assert record["exact_data_citation"] is True
    assert record["rounded_data_citation"] is True
    assert record["high_citation_rate"] is True
    assert record["published_type"] == "IC50"
    assert record["published_units"] == "nM"
    assert record["standard_units"] == "µM"
    assert record["chembl_release"] == "33"
    expected_properties = (
        '[{"name":"Citation Count","value":51},'
        '{"name":"Exact Data Citation","value":true}]'
    )
    assert record["activity_properties"] == expected_properties
