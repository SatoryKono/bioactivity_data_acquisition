import json

import pytest

from bioetl.sources.chembl.testitem.parser import TestItemParser
from bioetl.sources.chembl.testitem.pipeline import TestItemPipeline


@pytest.fixture()
def parser() -> TestItemParser:
    return TestItemParser(
        expected_columns=TestItemPipeline._expected_columns(),
        property_fields=TestItemPipeline._CHEMBL_PROPERTY_FIELDS,
        structure_fields=TestItemPipeline._CHEMBL_STRUCTURE_FIELDS,
        json_fields=TestItemPipeline._CHEMBL_JSON_FIELDS,
        text_fields=TestItemPipeline._CHEMBL_TEXT_FIELDS,
        fallback_fields=TestItemPipeline._FALLBACK_FIELDS,
    )


def test_parse_normalizes_fields(parser: TestItemParser) -> None:
    payload = {
        "molecule_chembl_id": "CHEMBL25",
        "molregno": 123,
        "pref_name": " Aspirin ",
        "molecule_properties": {
            "ro3_pass": "yes",
            "lipinski_ro5_pass": "0",
            "num_rotatable_bonds": 5,
        },
        "molecule_structures": {
            "canonical_smiles": "CC(=O)OC1=CC=CC=C1C(=O)O",
            "standard_inchi": "InChI=1S/C9H8O4/c1-6(10)13-8-5-3-2-4-7(8)9(11)12/h2-5H,1H3,(H,11,12)",
            "standard_inchi_key": "BSYNRYMUTXBXSQ-UHFFFAOYSA-N",
        },
        "molecule_synonyms": [
            {"molecule_synonym": " Aspirin"},
            {"molecule_synonym": "ASA"},
        ],
        "atc_classifications": ["B01AC06"],
    }

    record = parser.parse(payload)

    assert record["pref_name"] == " Aspirin "
    assert record["pref_name_key"] == "aspirin"
    assert record["ro3_pass"] is True
    assert record["lipinski_ro5_pass"] is False
    assert record["rtb"] == 5

    synonyms = json.loads(record["molecule_synonyms"]) if record["molecule_synonyms"] else []
    synonym_values = {
        entry["molecule_synonym"] if isinstance(entry, dict) else entry for entry in synonyms
    }
    assert synonym_values == {"Aspirin", "ASA"}
    assert record["all_names"] == "ASA; Aspirin"

    serialized_atc = record["atc_classifications"]
    assert serialized_atc is not None
    assert json.loads(serialized_atc) == ["B01AC06"]

    for field in TestItemPipeline._FALLBACK_FIELDS:
        assert record[field] is None
