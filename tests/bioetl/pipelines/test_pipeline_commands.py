from __future__ import annotations

from pathlib import Path

import pandas as pd

from bioetl.core.pipeline.factory import StageFactory
from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.types import (
    MaterializationConfig,
    PipelineConfig,
    PipelineInfo,
    StageContext,
    StageExecutionOptions,
    WriteArtifacts,
)
from bioetl.core.pipeline.orchestration import PipelineBaseCommon


class CommandSpyPipeline(PipelineBaseCommon):
    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        super().__init__(config, run_id)
        self.calls: list[str] = []

    def prepare_run(self, options: StageExecutionOptions) -> None:
        self.calls.append("prepare_run")

    def extract(self, descriptor: object, options: StageExecutionOptions) -> pd.DataFrame:
        self.calls.append("extract")
        return pd.DataFrame({"value": [1]})

    def transform(self, df: pd.DataFrame, options: StageExecutionOptions) -> pd.DataFrame:
        self.calls.append("transform")
        return df

    def validate(self, df: pd.DataFrame, options: StageExecutionOptions) -> pd.DataFrame:
        self.calls.append("validate")
        return df

    def save_results(
        self, df: pd.DataFrame, artifacts: WriteArtifacts, options: StageExecutionOptions
    ) -> pd.DataFrame:
        self.calls.append("save_results")
        return df

    def finalize_run(self, run_result) -> None:
        self.calls.append("finalize_run")


CONFIG = PipelineConfig(
    pipeline=PipelineInfo(name="spy"),
    materialization=MaterializationConfig(root=Path("/tmp/out")),
)
OPTIONS = StageExecutionOptions(run_tag=None, mode=None)


def _stage_context(pipeline: PipelineBaseCommon) -> StageContext:
    logger = UnifiedLogger.get("StageFactoryTest")
    return StageContext(
        pipeline=pipeline,
        output_dir=Path("/tmp/out"),
        logger=logger,
        run_id="test",
        run_tag=None,
        mode=None,
        artifacts=WriteArtifacts(),
    )


def test_default_stage_plan_contains_all_steps() -> None:
    pipeline = CommandSpyPipeline(CONFIG, run_id="spy-1")
    factory = StageFactory(pipeline.pipeline_definition)
    plan = factory.build(_stage_context(pipeline), OPTIONS)

    assert [cmd.name for cmd in plan] == ["extract", "transform", "validate", "save_results"]


def test_partial_plan_respects_requested_stages() -> None:
    pipeline = CommandSpyPipeline(CONFIG, run_id="spy-2")
    factory = StageFactory(pipeline.pipeline_definition)
    plan = factory.build(_stage_context(pipeline), OPTIONS, stages=["extract", "validate"])

    assert [cmd.name for cmd in plan] == ["extract", "validate"]


def test_dry_run_skips_save_results_stage() -> None:
    pipeline = CommandSpyPipeline(CONFIG, run_id="spy-3")
    factory = StageFactory(pipeline.pipeline_definition)
    context = _stage_context(pipeline)
    plan = factory.build(context, StageExecutionOptions(run_tag=None, mode=None, dry_run=True))

    assert "save_results" not in [cmd.name for cmd in plan]
