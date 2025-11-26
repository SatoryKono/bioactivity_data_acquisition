"""Tests for pipeline command execution and stage planning."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from bioetl.core.logging import UnifiedLogger
from bioetl.core.pipeline.factory import StageFactory
from bioetl.core.pipeline.unified import UnifiedPipelineBase
from bioetl.core.pipeline.stage_plan import (
    StagePlanMetadata,
    build_default_stage_plan,
)
from bioetl.core.pipeline.types import (
    DefaultArtifactContext,
    DefaultDomainContext,
    DefaultExecutionContext,
    DefaultInfrastructureContext,
    MaterializationConfig,
    PipelineConfig,
    PipelineInfo,
    StageContext,
    StageDescriptor,
    StageExecutionOptions,
    StageRuntimeContext,
    WriteArtifacts,
)


class CommandSpyPipeline(UnifiedPipelineBase):
    """Test pipeline that records method calls."""

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        if not hasattr(self, "validator"):
            self.validator = None
        super().__init__(config, run_id=run_id)
        self.calls: list[str] = []

    def prepare_run(self, options: StageExecutionOptions) -> None:
        """Record prepare_run call."""
        self.calls.append("prepare_run")

    def extract(
        self, descriptor: object, options: StageExecutionOptions
    ) -> pd.DataFrame:
        """Record extract call."""
        self.calls.append("extract")
        return pd.DataFrame({"value": [1]})

    def transform(
        self, df: pd.DataFrame, options: StageExecutionOptions
    ) -> pd.DataFrame:
        """Record transform call."""
        self.calls.append("transform")
        return df

    def validate(
        self, df: pd.DataFrame, options: StageExecutionOptions
    ) -> pd.DataFrame:
        """Record validate call."""
        self.calls.append("validate")
        return df

    def save_results(
        self,
        df: pd.DataFrame,
        artifacts: WriteArtifacts,
        options: StageExecutionOptions,
    ) -> pd.DataFrame:
        """Record save_results call."""
        self.calls.append("save_results")
        return df

    def finalize_run(self, run_result) -> None:
        """Record finalize_run call."""
        self.calls.append("finalize_run")


class ValidatingCommandSpyPipeline(CommandSpyPipeline):
    """Test pipeline with a validator."""

    def __init__(self, config: PipelineConfig, run_id: str) -> None:
        self.validator = object()
        super().__init__(config, run_id=run_id)
        self.validator = object()

    def build_stage_plan(
        self, context: StageContext, options: StageExecutionOptions
    ) -> tuple[StageDescriptor, ...]:
        """Build stage plan with validator."""
        metadata = StagePlanMetadata(
            dry_run=options.dry_run,
            has_validator=True,
        )
        return tuple(build_default_stage_plan(context.descriptor, metadata))


CONFIG = PipelineConfig(
    pipeline=PipelineInfo(name="spy"),
    materialization=MaterializationConfig(root=Path("/tmp/out")),
)
OPTIONS = StageExecutionOptions(run_tag=None, mode=None)


def _contexts(
    pipeline: UnifiedPipelineBase,
) -> tuple[StageContext, StageRuntimeContext]:
    logger = UnifiedLogger.get("StageFactoryTest")
    context = StageContext(
        execution=DefaultExecutionContext(logger=logger, request_id="test"),
        domain=DefaultDomainContext(pipeline=pipeline),
        infrastructure=DefaultInfrastructureContext(output_dir=Path("/tmp/out")),
        artifacts=DefaultArtifactContext(),
        config_provider=lambda _k: {},
    )
    runtime = StageRuntimeContext(context=context, options=OPTIONS)
    return context, runtime


def test_default_stage_plan_contains_all_steps() -> None:
    """Test that default plan includes all standard stages."""
    pipeline = CommandSpyPipeline(CONFIG, run_id="spy-1")
    factory = StageFactory(pipeline)
    context, _ = _contexts(pipeline)
    descriptors = pipeline.build_stage_plan(context, OPTIONS)
    plan = factory.build(descriptors, context, OPTIONS)

    assert [cmd.name for cmd in plan] == [
        "extract",
        "transform",
        "validate",
        "save_results",
    ]


def test_partial_plan_respects_requested_stages() -> None:
    """Test that plan builder respects requested stages."""
    pipeline = CommandSpyPipeline(CONFIG, run_id="spy-2")
    factory = StageFactory(pipeline)
    context, _ = _contexts(pipeline)
    descriptors = pipeline.build_stage_plan(context, OPTIONS)
    plan = factory.build(
        descriptors,
        context,
        OPTIONS,
        stages=["extract", "validate"],
    )

    assert [cmd.name for cmd in plan] == ["extract", "validate"]


def test_dry_run_skips_save_results_stage() -> None:
    """Test that dry run omits save_results."""
    pipeline = CommandSpyPipeline(CONFIG, run_id="spy-3")
    factory = StageFactory(pipeline)
    context, runtime = _contexts(pipeline)
    runtime.options = StageExecutionOptions(
        run_tag=None, mode=None, dry_run=True
    )
    descriptors = pipeline.build_stage_plan(context, runtime.options)
    plan = factory.build(descriptors, context, runtime.options)

    assert "save_results" not in [cmd.name for cmd in plan]


def test_dry_run_with_validator_retains_validation_steps() -> None:
    """Test that dry run with validator keeps validation."""
    pipeline = ValidatingCommandSpyPipeline(CONFIG, run_id="spy-4")
    factory = StageFactory(pipeline)
    context, runtime = _contexts(pipeline)
    runtime.options = StageExecutionOptions(
        run_tag=None, mode=None, dry_run=True
    )
    descriptors = pipeline.build_stage_plan(context, runtime.options)
    plan = factory.build(descriptors, context, runtime.options)

    assert [cmd.name for cmd in plan] == ["extract", "transform", "validate"]
