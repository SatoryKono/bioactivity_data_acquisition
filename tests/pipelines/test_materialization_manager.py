from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

from bioetl.config.models import MaterializationPaths
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
