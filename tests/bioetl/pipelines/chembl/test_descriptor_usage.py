"""Regression tests ensuring every ChEMBL pipeline uses descriptors."""

from __future__ import annotations

import pandas as pd
import pytest

from bioetl.chembl.common.descriptor import (
    ChemblExtractionDescriptor,
    ChemblPipelineBase,
)
from bioetl.config.models.models import PipelineConfig
from bioetl.pipelines.chembl.activity.run import ChemblActivityPipeline
from bioetl.pipelines.chembl.assay.run import ChemblAssayPipeline
from bioetl.pipelines.chembl.document.run import ChemblDocumentPipeline
from bioetl.pipelines.chembl.target.run import ChemblTargetPipeline
from bioetl.pipelines.chembl.testitem.run import TestItemChemblPipeline

PipelineType = type[ChemblPipelineBase]

ALL_PIPELINES: tuple[PipelineType, ...] = (
    ChemblActivityPipeline,
    ChemblAssayPipeline,
    ChemblDocumentPipeline,
    ChemblTargetPipeline,
    TestItemChemblPipeline,
)


@pytest.mark.parametrize("pipeline_cls", ALL_PIPELINES)
def test_build_descriptor_returns_extraction_descriptor(
    pipeline_cls: PipelineType,
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    """Every ChEMBL pipeline must provide a descriptor implementation."""

    pipeline = pipeline_cls(config=pipeline_config_fixture, run_id=run_id)
    descriptor = pipeline.build_descriptor()

    assert isinstance(descriptor, ChemblExtractionDescriptor)
    assert descriptor.name.startswith("chembl_")


@pytest.mark.parametrize("pipeline_cls", ALL_PIPELINES)
def test_extract_all_delegates_to_run_extract_all(
    pipeline_cls: PipelineType,
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``extract_all`` must route through ``ChemblPipelineBase.run_extract_all``."""

    pipeline = pipeline_cls(config=pipeline_config_fixture, run_id=run_id)

    sentinel = pd.DataFrame({"ok": [1]})
    captured_descriptor: ChemblExtractionDescriptor | None = None

    def fake_run_extract_all(
        self: ChemblPipelineBase,
        descriptor: ChemblExtractionDescriptor,
    ) -> pd.DataFrame:  # pragma: no cover - invoked via pipeline.extract_all
        nonlocal captured_descriptor
        captured_descriptor = descriptor
        return sentinel

    monkeypatch.setattr(ChemblPipelineBase, "run_extract_all", fake_run_extract_all)

    result = pipeline.extract_all()

    assert result is sentinel
    assert isinstance(captured_descriptor, ChemblExtractionDescriptor)
