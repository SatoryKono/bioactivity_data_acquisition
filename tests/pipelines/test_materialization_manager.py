from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

from bioetl.config.models import DeterminismConfig, MaterializationPaths
from bioetl.core.materialization import MaterializationManager

pytestmark = pytest.mark.integration


def test_materialization_manager_creates_expected_csv_files(tmp_path) -> None:
    """Silver stage materialization should sort data and emit CSV artefacts."""

    paths = MaterializationPaths.model_validate(
        {
            "root": tmp_path,
            "stages": {
                "silver": {
                    "directory": "silver",
                    "datasets": {
                        "uniprot": {"filename": "targets_silver"},
                        "component_enrichment": {"filename": "component_enrichment"},
                    },
                },
                "gold": {
                    "directory": "gold",
                    "datasets": {
                        "targets": {"filename": "targets_final"},
                        "target_components": {"filename": "target_components"},
                        "protein_class": {"filename": "protein_class"},
                        "target_xref": {"filename": "target_xref"},
                    },
                },
            },
        }
    )
    stage_context: dict[str, Any] = {}
    manager = MaterializationManager(paths, runtime=None, stage_context=stage_context)

    uniprot_df = pd.DataFrame(
        {
            "canonical_accession": ["P99999", "A00001"],
            "value": [2, 1],
        }
    )
    component_df = pd.DataFrame(
        {
            "canonical_accession": ["P99999", "A00001"],
            "isoform_accession": ["P99999-2", "A00001-1"],
        }
    )

    outputs = manager.materialize_silver(uniprot_df, component_df, format="csv")

    assert set(outputs) == {"uniprot", "component_enrichment"}
    assert all(path.suffix == ".csv" for path in outputs.values())
    assert all(path.exists() for path in outputs.values())

    written_uniprot = pd.read_csv(outputs["uniprot"])
    assert written_uniprot["canonical_accession"].tolist() == ["A00001", "P99999"]

    written_components = pd.read_csv(outputs["component_enrichment"])
    assert written_components["isoform_accession"].tolist() == ["A00001-1", "P99999-2"]

    materialization_ctx = stage_context.get("materialization", {})
    assert "silver" in materialization_ctx
    assert materialization_ctx["silver"].get("outputs") == outputs


def test_materialization_manager_respects_determinism(tmp_path) -> None:
    """CSV materialization should respect determinism float/date formats."""

    determinism = DeterminismConfig(
        float_precision=4,
        datetime_format="%Y/%m/%d %H:%M:%S",
    )

    paths = MaterializationPaths.model_validate(
        {
            "root": tmp_path,
            "stages": {
                "silver": {
                    "directory": "silver",
                    "datasets": {
                        "uniprot": {"filename": "targets_silver"},
                    },
                }
            },
        }
    )

    manager = MaterializationManager(
        paths,
        runtime=None,
        stage_context={},
        determinism=determinism,
    )

    uniprot_df = pd.DataFrame(
        {
            "canonical_accession": ["P99999", "A00001"],
            "measurement_value": [3.1415926535, 2.5],
            "recorded_at": pd.to_datetime(
                ["2024-03-01 10:15:30", "2024-03-01 11:45:00"]
            ),
        }
    )

    outputs = manager.materialize_silver(uniprot_df, pd.DataFrame(), format="csv")
    uniprot_path = outputs["uniprot"]

    expected = Path(__file__).with_name("golden") / "silver_uniprot_deterministic.csv"
    assert uniprot_path.read_text(encoding="utf-8") == expected.read_text(encoding="utf-8")


def test_materialization_manager_respects_dry_run(tmp_path) -> None:
    """Dry-run runtime option should prevent writing any gold artefacts."""

    runtime = SimpleNamespace(dry_run=True)
    paths = MaterializationPaths.model_validate(
        {
            "root": tmp_path,
            "stages": {
                "gold": {
                    "directory": "gold",
                    "datasets": {
                        "targets": {"filename": "targets_final"},
                        "target_components": {"filename": "target_components"},
                        "protein_class": {"filename": "protein_class"},
                        "target_xref": {"filename": "target_xref"},
                    },
                }
            },
        }
    )
    stage_context: dict[str, Any] = {}
    manager = MaterializationManager(paths, runtime=runtime, stage_context=stage_context)

    targets = pd.DataFrame({"target_chembl_id": ["CHEMBL1"]})
    components = pd.DataFrame({"target_chembl_id": ["CHEMBL1"], "component_id": [1]})
    protein_class = pd.DataFrame(
        {
            "target_chembl_id": ["CHEMBL1"],
            "class_level": ["L1"],
            "class_name": ["Kinase"],
        }
    )
    xref = pd.DataFrame(
        {
            "target_chembl_id": ["CHEMBL1"],
            "xref_src_db": ["Ensembl"],
            "xref_id": ["ENSG0001"],
        }
    )

    outputs = manager.materialize_gold(
        targets,
        components,
        protein_class,
        xref,
        format="csv",
    )

    assert outputs == {}
    expected_dir = tmp_path / "gold"
    assert not expected_dir.exists()

    materialization_ctx = stage_context.get("materialization", {})
    gold_context = materialization_ctx.get("gold", {})
    assert gold_context.get("status") == "skipped"
    assert gold_context.get("reason") == "dry_run"


def test_materialization_manager_cleans_up_after_failed_write(tmp_path, monkeypatch) -> None:
    """Atomic writes prevent partial CSV artefacts and remove temp files on failure."""

    paths = MaterializationPaths.model_validate(
        {
            "root": tmp_path,
            "stages": {
                "silver": {
                    "directory": "silver",
                    "datasets": {
                        "uniprot": {"filename": "targets_silver"},
                    },
                }
            },
        }
    )

    manager = MaterializationManager(paths, stage_context={}, run_id="failure-test")

    df = pd.DataFrame({"canonical_accession": ["P99999"], "value": [1]})

    def boom(self, *args, **kwargs):  # noqa: ANN001 - mirrors DataFrame.to_csv signature
        raise RuntimeError("boom")

    monkeypatch.setattr(pd.DataFrame, "to_csv", boom, raising=False)

    with pytest.raises(RuntimeError, match="boom"):
        manager.materialize_silver(df, pd.DataFrame(), format="csv")

    dataset_path = tmp_path / "silver" / "targets_silver.csv"
    assert not dataset_path.exists(), "no artefact should remain after a failed write"
    assert not any(tmp_path.rglob("*.tmp")), "temporary CSV files must be removed"
    assert not any(tmp_path.rglob(".tmp_run_failure-test")), "run-scoped temp dir should be cleaned"
