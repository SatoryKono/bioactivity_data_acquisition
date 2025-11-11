"""Shared assertions for ChEMBL pipeline input-file branching."""

from __future__ import annotations

from typing import Type
from unittest.mock import patch

import pandas as pd
import pytest

from bioetl.config import PipelineConfig
from bioetl.pipelines.chembl.activity.run import ChemblActivityPipeline
from bioetl.pipelines.chembl.assay.run import ChemblAssayPipeline
from bioetl.pipelines.chembl.document.run import ChemblDocumentPipeline
from bioetl.pipelines.chembl.target.run import ChemblTargetPipeline
from bioetl.pipelines.chembl.testitem.run import TestItemChemblPipeline as ChemblTestItemPipeline
from bioetl.pipelines.chembl_base import ChemblPipelineBase


@pytest.mark.unit
@pytest.mark.parametrize(
    ("pipeline_cls", "pipeline_name", "event_name"),
    [
        (ChemblActivityPipeline, "activity_chembl", "chembl_activity.extract_mode"),
        (ChemblAssayPipeline, "assay_chembl", "chembl_assay.extract_mode"),
        (ChemblDocumentPipeline, "document_chembl", "chembl_document.extract_mode"),
        (ChemblTestItemPipeline, "testitem_chembl", "chembl_testitem.extract_mode"),
        (ChemblTargetPipeline, "target_chembl", "chembl_target.extract_mode"),
    ],
)
def test_extract_uses_shared_input_file_helper(
    pipeline_cls: Type[ChemblPipelineBase],
    pipeline_name: str,
    event_name: str,
    pipeline_config_fixture: PipelineConfig,
    run_id: str,
) -> None:
    """Ensure all ChEMBL pipelines delegate input-file handling to the helper."""

    config = pipeline_config_fixture.model_copy(deep=True)
    config.pipeline.name = pipeline_name
    config.cli.input_file = "ids.csv"

    pipeline = pipeline_cls(config=config, run_id=run_id)
    expected = pd.DataFrame({"_": []})

    with (
        patch.object(
            ChemblPipelineBase,
            "_dispatch_extract_mode",
            autospec=True,
            return_value=expected,
        ) as helper_mock,
        patch.object(pipeline_cls, "extract_all", autospec=True) as extract_all_mock,
    ):
        result = pipeline.extract()

    helper_mock.assert_called_once()
    helper_args, helper_kwargs = helper_mock.call_args
    assert helper_args[0] is pipeline
    assert len(helper_args) >= 2  # pipeline instance and bound logger
    assert helper_kwargs["event_name"] == event_name
    extract_all_mock.assert_not_called()
    assert result is expected
