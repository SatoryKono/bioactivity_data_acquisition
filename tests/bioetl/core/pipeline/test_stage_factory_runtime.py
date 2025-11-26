from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.factory import StageFactory
from bioetl.core.pipeline.unified import UnifiedPipelineBase
from bioetl.core.pipeline.stage_plan import (
    StagePlanMetadata,
    build_default_stage_plan,
)
from bioetl.core.pipeline.types import (
    ArtifactStore,
    MaterializationConfig,
    PipelineConfig,
    PipelineInfo,
    StageContext,
    StageDescriptor,
    StageExecutionOptions,
    StageRuntimeContext,
    StageResult,
    WriteArtifacts,
    WriteResult,
)


class CommandSpyPipeline(UnifiedPipelineBase):
    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        self.validator = None
        super().__init__(config, run_id=run_id)
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
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
    ) -> WriteResult:
        self.calls.append("save_results")
        return WriteResult(rows=len(df), artifacts=artifacts)

    def finalize_run(self, run_result) -> None:
        self.calls.append("finalize_run")


CONFIG = PipelineConfig(
    pipeline=PipelineInfo(name="spy"),
    materialization=MaterializationConfig(root=Path("/tmp/out")),
)
OPTIONS = StageExecutionOptions(run_tag=None, mode=None)


def _stage_context(pipeline: UnifiedPipelineBase) -> StageContext:
    logger = UnifiedLogger.get("StageFactoryTest")
    return StageContext(
        pipeline=pipeline,
        logger=logger,
        request_id="test",
        output_dir=Path("/tmp/out"),
        artifact_store=ArtifactStore(WriteArtifacts()),
    )


def test_stage_factory_executes_pipeline_methods() -> None:
    pipeline = CommandSpyPipeline(CONFIG, run_id="spy-1")
    context = _stage_context(pipeline)
    descriptors = pipeline.build_stage_plan(context, OPTIONS)
    factory = StageFactory(pipeline)
    stages = factory.build(descriptors, context, OPTIONS)

    runtime_context = StageRuntimeContext(context=context, options=OPTIONS)
    for stage in stages:
        stage.execute(runtime_context)

    assert pipeline.calls == ["extract", "transform", "validate", "save_results"]


def test_stage_plan_respects_dry_run_without_validator() -> None:
    pipeline = CommandSpyPipeline(CONFIG, run_id="spy-2")
    context = _stage_context(pipeline)
    options = StageExecutionOptions(run_tag=None, mode=None, dry_run=True)
    descriptors = pipeline.build_stage_plan(context, options)

    assert [descriptor.id for descriptor in descriptors] == ["extract"]


class FakeStageFactory(StageFactory):
    def __init__(self, pipeline: UnifiedPipelineBase) -> None:
        super().__init__(pipeline)
        self.created_from: list[str] = []

    def build(
        self,
        descriptors: Iterable[StageDescriptor],
        context: StageContext,
        options: StageExecutionOptions,
        stages: list[str] | None = None,
    ):
        stages = []
        for descriptor in descriptors:
            self.created_from.append(descriptor.id)
            stages.append(_StubStage(descriptor.id))
        return tuple(stages)


class _StubStage:
    def __init__(self, name: str) -> None:
        self.name = name
        self.executed = False

    def execute(self, runtime_context: StageRuntimeContext) -> StageResult:
        self.executed = True
        if self.name == "extract":
            runtime_context.context.data_bucket.set(pd.DataFrame({"value": [1]}))
        if self.name == "save_results":
            artifacts = (
                runtime_context.context.artifact_store.get() or WriteArtifacts()
            )
            return StageResult(
                name=self.name,
                output=WriteResult(rows=1, artifacts=artifacts),
            )
        return StageResult(
            name=self.name,
            output=runtime_context.context.data_bucket.get(),
        )


class FactorySpyPipeline(CommandSpyPipeline):
    def __init__(
        self,
        config: PipelineConfig,
        run_id: str,
        factory: FakeStageFactory,
    ) -> None:
        super().__init__(config, run_id=run_id)
        self._factory = factory

    def create_stage_factory(self) -> StageFactory:
        return self._factory


def test_pipeline_runtime_uses_stage_factory() -> None:
    factory: FakeStageFactory | None = None

    class _Pipeline(FactorySpyPipeline):
        def __init__(self, cfg: PipelineConfig, rid: str) -> None:
            nonlocal factory
            factory = FakeStageFactory(self)
            super().__init__(cfg, rid, factory)

        def build_stage_plan(
            self, context: StageContext, options: StageExecutionOptions
        ) -> tuple[StageDescriptor, ...]:
            metadata = StagePlanMetadata(
                dry_run=options.dry_run,
                has_validator=True,
            )
            return tuple(build_default_stage_plan(context.descriptor, metadata))

    pipeline = _Pipeline(CONFIG, "spy-3")
    result = pipeline.run(Path("/tmp/out"))

    assert result.success is True
    assert factory is not None
    assert factory.created_from == [
        "extract",
        "transform",
        "validate",
        "save_results",
    ]
