"""Regression tests for the ChEMBL document pipeline."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from bioetl.chembl.common.descriptor import (
    ChemblExtractionContext,
    ChemblExtractionDescriptor,
)
from bioetl.config.models.models import PipelineConfig
from bioetl.pipelines.chembl.document.run import ChemblDocumentPipeline


def test_descriptor_dry_run_for_document(
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
    monkeypatch,
) -> None:
    """Ensure descriptor-driven dry-run logic short-circuits extraction."""

    config = pipeline_config_fixture.model_copy(deep=True)
    config.pipeline = config.pipeline.model_copy(update={"name": "document_chembl"})
    config.cli.dry_run = True

    pipeline = ChemblDocumentPipeline(config=config, run_id=run_id)

    dry_run_invoked = {"flag": False}

    def build_context(pipeline_obj, source_config, log):  # type: ignore[no-untyped-def]
        context = ChemblExtractionContext(
            source_config=source_config,
            iterator=MagicMock(),
        )
        context.page_size = 25
        context.chembl_release = "33"
        context.metadata = {"source": "chembl"}
        context.stats = {"pages": 0}
        return context

    def dry_run_handler(pipeline_obj, context, log, stage_start):  # type: ignore[no-untyped-def]
        dry_run_invoked["flag"] = True
        return pd.DataFrame({pipeline_obj.id_column: pd.Series(dtype="string")})

    descriptor = ChemblExtractionDescriptor[ChemblDocumentPipeline](  # type: ignore[type-arg]
        name="chembl_document",
        source_name="chembl",
        source_config_factory=lambda cfg: cfg,
        build_context=build_context,
        id_column="document_chembl_id",
        summary_event="chembl_document.extract_summary",
        dry_run_handler=dry_run_handler,
    )

    monkeypatch.setattr(ChemblDocumentPipeline, "build_descriptor", lambda self: descriptor)

    frame = pipeline.extract_all()

    assert dry_run_invoked["flag"] is True
    assert frame.empty
