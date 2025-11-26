from __future__ import annotations

from pathlib import Path
from unittest import mock

import pandas as pd

from bioetl.core.pipeline.stage_plan import StagePlanMetadata
from bioetl.core.pipeline.types import StageExecutionOptions
from bioetl.core.pipeline.unified import UnifiedPipelineBase


class DelegatingPipeline(UnifiedPipelineBase):
    def extract(self, descriptor, options: StageExecutionOptions):  # pragma: no cover - unused
        return pd.DataFrame()

    def transform(self, df: pd.DataFrame, options: StageExecutionOptions):  # pragma: no cover - unused
        return df

    def validate(self, df: pd.DataFrame, options: StageExecutionOptions):  # pragma: no cover - unused
        return df


def test_build_stage_plan_delegates_to_default_plan():
    pipeline = DelegatingPipeline(config={}, run_id="delegate")
    context = mock.Mock()
    options = StageExecutionOptions(run_tag=None, mode=None)

    with mock.patch(
        "bioetl.core.pipeline.unified.build_default_stage_plan",
    ) as build_plan:
        pipeline.build_stage_plan(context, options)

    build_plan.assert_called_once()
    descriptor_arg, metadata_arg = build_plan.call_args[0]
    assert descriptor_arg is context.descriptor
    assert isinstance(metadata_arg, StagePlanMetadata)
    assert metadata_arg.dry_run is False
    assert metadata_arg.has_validator is False


def test_run_uses_runtime_factory_spy(tmp_path: Path):
    pipeline = DelegatingPipeline(config={}, run_id="delegate")
    with mock.patch.object(pipeline, "build_stage_plan", wraps=pipeline.build_stage_plan) as spy:
        pipeline.run(tmp_path, dry_run=True)

    spy.assert_called()
