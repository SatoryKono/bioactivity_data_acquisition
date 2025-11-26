"""Smoke-тесты сухого прогона ChEMBL-пайплайнов."""
from __future__ import annotations

from pathlib import Path
from typing import Type

import pytest

from bioetl.pipelines.chembl.assay.run import ChemblAssayPipeline
from bioetl.pipelines.chembl.document.run import ChemblDocumentPipeline
from bioetl.pipelines.chembl.target.run import ChemblTargetPipeline
from bioetl.pipelines.chembl.testitem.run import TestItemChemblPipeline


def _build_base_config(
    sort_field: str,
    *,
    batch_size: int = 10,
    extra: dict | None = None,
) -> dict:
    base = {
        "sources": {
            "chembl": {
                "batch_size": batch_size,
                "max_url_length": 1500,
            },
        },
        "cache": {"namespace": "chembl-smoke"},
        "determinism": {"sort": {"by": [sort_field]}},
    }
    if extra:
        base.update(extra)
    return base


@pytest.mark.parametrize(
    "pipeline_cls,sort_field,extra",
    [
        (
            ChemblAssayPipeline,
            "assay_chembl_id",
            {"postprocess": {"nested_serialization": "flatten"}},
        ),
        (
            ChemblDocumentPipeline,
            "document_chembl_id",
            {"mode": "all", "fallbacks": {"policy": "ordered"}},
        ),
        (ChemblTargetPipeline, "target_chembl_id", {}),
        (TestItemChemblPipeline, "test_item_id", {}),
    ],
)
def test_chembl_pipeline_dry_run(
    pipeline_cls: Type, sort_field: str, extra: dict, tmp_path: Path
):
    config = _build_base_config(
        sort_field,
        batch_size=20 if pipeline_cls is ChemblAssayPipeline else 10,
        extra=extra,
    )
    pipeline = pipeline_cls(config)

    result = pipeline.run(tmp_path, dry_run=True, sample=3)

    assert result.success
    assert result.metrics["rows"] == 0
    # В режиме dry_run стадия save_results пропускается, поэтому файл не создаётся
    # assert Path(result.metrics["output_path"]).exists()


__all__ = ["test_chembl_pipeline_dry_run"]
