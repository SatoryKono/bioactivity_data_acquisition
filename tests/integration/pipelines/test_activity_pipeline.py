"""Golden tests for the Activity pipeline determinism contract."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bioetl.config.loader import load_config
from bioetl.pipelines.activity import ActivityPipeline
from bioetl.schemas import ActivitySchema
from bioetl.utils.chembl import ChemblRelease

GOLDEN_COLUMN_ORDER_PATH = (
    Path(__file__).resolve().parent / "golden" / "activity_column_order.json"
)


def _load_golden_column_order() -> list[str]:
    with GOLDEN_COLUMN_ORDER_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _build_activity_dataframe() -> pd.DataFrame:
    """Create a minimal frame that satisfies the Activity schema."""

    row = {
        "activity_id": 1,
        "molecule_chembl_id": "CHEMBL1",
        "assay_chembl_id": "CHEMBL2",
        "target_chembl_id": "CHEMBL3",
        "document_chembl_id": "CHEMBL4",
        "published_type": "IC50",
        "published_relation": "=",
        "published_value": 10.0,
        "published_units": "nM",
        "standard_type": "IC50",
        "standard_relation": "=",
        "standard_value": 9.5,
        "standard_units": "nM",
        "standard_flag": 1,
        "pchembl_value": 7.0,
        "lower_bound": 8.0,
        "upper_bound": 11.0,
        "is_censored": False,
        "activity_comment": None,
        "data_validity_comment": None,
        "bao_endpoint": "BAO_0000190",
        "bao_format": "BAO_0000357",
        "bao_label": "single protein format",
        "potential_duplicate": 0,
        "uo_units": "UO_0000065",
        "qudt_units": "http://qudt.org/vocab/unit/NanoMOL-PER-L",
        "src_id": 1,
        "action_type": "inhibition",
        "canonical_smiles": "C1=CC=CC=C1",
        "target_organism": "Homo sapiens",
        "target_tax_id": 9606,
        "activity_properties": "[]",
        "compound_key": "CHEMBL1|IC50|CHEMBL3",
        "is_citation": True,
        "high_citation_rate": False,
        "exact_data_citation": False,
        "rounded_data_citation": False,
        "bei": 1.0,
        "sei": 1.0,
        "le": 1.0,
        "lle": 1.0,
    }

    columns = [col for col in ActivitySchema.get_column_order() if col in row]
    return pd.DataFrame([row], columns=columns)


def test_activity_column_order_matches_schema_and_golden() -> None:
    """Config determinism order should equal schema column order and golden snapshot."""

    config = load_config("configs/pipelines/activity.yaml")
    config_order = list(config.determinism.column_order)
    golden_order = _load_golden_column_order()
    schema_order = ActivitySchema.get_column_order()

    assert config_order == golden_order
    assert config_order == schema_order


def test_activity_pipeline_output_matches_golden(monkeypatch) -> None:
    """Activity pipeline output must follow the golden column order without omissions."""

    stub_release = ChemblRelease(
        version="ChEMBL_TEST",
        status={"chembl_db_version": "ChEMBL_TEST"},
    )
    monkeypatch.setattr(
        "bioetl.pipelines.base.fetch_chembl_release",
        lambda _client: stub_release,
    )

    config = load_config("configs/pipelines/activity.yaml")
    pipeline = ActivityPipeline(config, run_id="golden-run")

    df = _build_activity_dataframe()
    transformed = pipeline.transform(df)
    validated = pipeline.validate(transformed)

    golden_order = _load_golden_column_order()

    assert list(validated.columns) == golden_order
    # Sanity check: every expected column is present exactly once.
    missing = [column for column in golden_order if column not in validated.columns]
    assert not missing

