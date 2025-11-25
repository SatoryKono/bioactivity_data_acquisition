"""StageFactory for building pipeline stage plans."""
from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from bioetl.core.pipeline.types import (
    PipelineStageCommand,
    PipelineStagesProtocol,
    StageContext,
    StageExecutionOptions,
)


class StageFactory:
    """Factory that builds a sequence of :class:`PipelineStageCommand` objects."""

    def __init__(self, pipeline: PipelineStagesProtocol) -> None:
        self.pipeline = pipeline

    def build(
        self, options: StageExecutionOptions, stages: Sequence[str] | None = None
    ) -> tuple[PipelineStageCommand, ...]:
        """Build a stage plan.

        Args:
            options: Shared execution options.
            stages: Explicit list of stages to include. If ``None`` the default
                pipeline plan is used.
        """

        plan = stages or [
            "extract",
            "transform",
            "validate",
            "save_results",
        ]
        if options.dry_run:
            plan = [stage for stage in plan if stage != "save_results"]
        commands = [self._command_for(stage) for stage in plan]
        return tuple(cmd for cmd in commands if cmd is not None)

    def _command_for(self, stage: str) -> PipelineStageCommand:
        if stage == "extract":
            return PipelineStageCommand("extract", self._run_extract)
        if stage == "transform":
            return PipelineStageCommand("transform", self._run_transform)
        if stage == "validate":
            return PipelineStageCommand("validate", self._run_validate)
        if stage == "save_results":
            return PipelineStageCommand("save_results", self._run_save_results)
        raise ValueError(f"Unknown stage '{stage}'")

    def _run_extract(self, context: StageContext, options: StageExecutionOptions) -> Any:
        descriptor = context.descriptor
        context.current_df = self.pipeline.extract(descriptor, options)
        return context.current_df

    def _run_transform(self, context: StageContext, options: StageExecutionOptions) -> Any:
        if context.current_df is None:
            raise RuntimeError("transform stage requires extracted data")
        context.current_df = self.pipeline.transform(context.current_df, options)
        return context.current_df

    def _run_validate(self, context: StageContext, options: StageExecutionOptions) -> Any:
        if context.current_df is None:
            raise RuntimeError("validate stage requires transformed data")
        context.current_df = self.pipeline.validate(context.current_df, options)
        return context.current_df

    def _run_save_results(self, context: StageContext, options: StageExecutionOptions) -> Any:
        if context.current_df is None:
            raise RuntimeError("save_results stage requires validated data")
        artifacts = context.artifacts
        if artifacts is None:
            from bioetl.core.pipeline.types import WriteArtifacts

            artifacts = WriteArtifacts()
            context.artifacts = artifacts
        result = self.pipeline.save_results(context.current_df, artifacts, options)
        context.metadata.setdefault("write_result", result)
        return result


__all__ = ["StageFactory"]
