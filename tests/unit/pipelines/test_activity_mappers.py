import json

import pandas as pd
import pytest

from bioetl.config.loader import load_config
from bioetl.pipelines import ActivityPipeline
from bioetl.pipelines.activity_mappers import (
    map_activity_row,
    normalize_activity_properties,
    normalize_ligand_efficiency,
)


@pytest.fixture(scope="module")
def activity_config():
    return load_config("configs/pipelines/activity.yaml")


def test_map_activity_row_normalizes_values():
    raw_row = {
        "activity_id": "123",
        "molecule_chembl_id": " chembl1 ",
        "assay_chembl_id": "CHEMBL999 ",
        "target_chembl_id": "chembl777",
        "document_chembl_id": "CHEMBL42",
        "standard_relation": "≤",
        "standard_units": "um",
        "standard_type": " Ki ",
        "published_units": "NM",
        "target_organism": "homo  sapiens",
        "activity_properties": [
            {"type": "Ki", "value": 10, "units": "nM"},
            {"name": "Comment", "value": "  High "},
        ],
        "ligand_efficiency": {"LE": 0.45, "LLE": 1.23456},
        "data_validity_comment": " outside range ",
        "bao_endpoint": "bao_0000190",
        "bao_format": " BAO_0000357 ",
        "compound_key": "",
        "high_citation_rate": "false",
        "exact_data_citation": "true",
        "rounded_data_citation": "0",
    }

    mapped = map_activity_row(raw_row)

    assert mapped["activity_id"] == 123
    assert mapped["molecule_chembl_id"] == "CHEMBL1"
    assert mapped["assay_chembl_id"] == "CHEMBL999"
    assert mapped["standard_relation"] == "<="
    assert mapped["standard_units"] == "µM"
    assert mapped["published_relation"] == "="
    assert mapped["target_organism"] == "Homo Sapiens"
    assert mapped["compound_key"].startswith("CHEMBL1|Ki|CHEMBL777")
    assert mapped["is_citation"] is True
    assert mapped["high_citation_rate"] is False
    assert mapped["exact_data_citation"] is True
    assert mapped["rounded_data_citation"] is False

    properties = mapped["activity_properties"].split("\n")
    assert "name=Comment|value=High" in properties
    assert "type=Ki|value=10.000000|units=nM" in properties

    eff_json = json.loads(mapped["ligand_efficiency"])
    assert eff_json["LE"] == pytest.approx(0.45)
    assert eff_json["LLE"] == pytest.approx(1.23456)
    assert eff_json["BEI"] is None
    assert eff_json["SEI"] is None
    assert pd.isna(mapped["bei"])
    assert pd.isna(mapped["sei"])


def test_normalize_activity_properties_from_json_string():
    payload = json.dumps(
        [
            {"name": "alpha", "value": 1},
            {"name": "beta", "value": 2, "relation": "≤"},
        ]
    )

    canonical = normalize_activity_properties(payload)
    rows = canonical.split("\n")
    assert "name=alpha|value=1.000000" in rows
    assert "name=beta|value=2.000000|relation=<=" in rows


def test_normalize_ligand_efficiency_empty_payload():
    canonical, metrics = normalize_ligand_efficiency(None)
    assert canonical == ""
    assert all(pd.isna(value) for value in metrics.values())


def test_transform_applies_na_policy(activity_config):
    pipeline = ActivityPipeline(activity_config, run_id="unit")
    raw_df = pd.DataFrame(
        [
            {
                "activity_id": 555,
                "molecule_chembl_id": "chembl555",
                "standard_relation": "≥",
                "standard_units": "uM",
                "activity_properties": [{"name": "note", "value": "  keep "}],
                "ligand_efficiency": {"SEI": 2.5},
                "document_chembl_id": "CHEMBL777",
            }
        ]
    )

    transformed = pipeline.transform(raw_df)

    assert transformed.at[0, "standard_relation"] == ">="
    assert transformed.at[0, "standard_units"] == "µM"
    assert transformed.at[0, "assay_chembl_id"] == ""
    assert transformed.at[0, "ligand_efficiency"]
    assert transformed.at[0, "activity_properties"].startswith("name=note")
    assert bool(transformed.at[0, "is_citation"]) is True
    assert bool(transformed.at[0, "high_citation_rate"]) is False
    assert transformed.at[0, "pipeline_version"] == "1.0.0"
    assert transformed.at[0, "source_system"] == "chembl"
    assert transformed.at[0, "hash_row"]
