from __future__ import annotations

import pandas as pd

from pipelines.cli import main
from pipelines.activity_chembl import ActivityChemblPipeline
from pipelines.documents_chembl import DocumentsChemblPipeline


def test_activity_pipeline_hashing(tmp_path):
    df = pd.DataFrame(
        [
            {
                "activity_id": 1,
                "row_subtype": "base",
                "row_index": 0,
                "assay_chembl_id": "A1",
                "testitem_chembl_id": "T1",
                "molecule_chembl_id": "M1",
            }
        ]
    )
    pipeline = ActivityChemblPipeline("run-1", config={"load_meta_id": "chembl-33"})

    transformed = pipeline.transform(df)

    assert transformed["hash_business_key"].iloc[0]
    assert len(transformed["hash_business_key"].iloc[0]) == 64
    assert transformed["hash_row"].iloc[0]
    assert len(transformed["hash_row"].iloc[0]) == 64
    assert transformed["load_meta_id"].iloc[0] == "chembl-33"


def test_documents_pipeline_enforces_source(tmp_path):
    df = pd.DataFrame(
        [
            {
                "document_chembl_id": "DOC1",
                "load_meta_id": "chembl-33",
            }
        ]
    )
    pipeline = DocumentsChemblPipeline(
        "run-2",
        config={"source_name": "chembl", "load_meta_id": "chembl-33"},
    )

    transformed = pipeline.transform(df)

    assert transformed["source"].iloc[0] == "chembl"
    assert transformed["hash_business_key"].iloc[0] is not None


def test_cli_runs_pipeline(tmp_path, monkeypatch):
    input_path = tmp_path / "testitems.csv"
    pd.DataFrame(
        [
            {
                "molecule_chembl_id": "M1",
                "_chembl_db_version": "33",
                "_api_version": "1",
            }
        ]
    ).to_csv(input_path, index=False)

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
        pipelines:
          testitems_chembl:
            load_meta_id: chembl-33
            source:
              path: {path}
        """.format(path=input_path)
    )

    exit_code = main(
        [
            "testitems_chembl",
            "--run-id",
            "run-cli",
            "--config",
            str(config_path),
            "--dry-run",
        ]
    )

    assert exit_code == 0
