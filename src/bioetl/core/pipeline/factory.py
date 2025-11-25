"""StageFactory for building pipeline stage plans."""
from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from bioetl.core.pipeline.stage_plan import build_default_stage_plan
from bioetl.core.pipeline.types import (
    PipelineBaseProtocol,
    PipelineStageCommand,
    StageContextProtocol,
    StageRuntimeContext,
)


class StageFactory:
    """Factory that builds a sequence of :class:`PipelineStageCommand` objects."""

    def __init__(self, pipeline: PipelineBaseProtocol) -> None:
        self.pipeline = pipeline

    def build(
        self,
        context: StageContextProtocol,
        runtime: StageRuntimeContext,
        stages: Sequence[str] | None = None,
    ) -> tuple[PipelineStageCommand, ...]:
        """Build a stage plan.

        Args:
            options: Shared execution options.
            stages: Explicit list of stages to include. If ``None`` the default
                pipeline plan is used.
        """

        stage_plan = build_default_stage_plan(self.pipeline, context, runtime)

        if stages is None:
            return stage_plan

        command_map = {command.name: command for command in stage_plan}
        filtered: list[PipelineStageCommand] = []
        for stage in stages:
            if stage not in command_map:
                if runtime.options.dry_run and stage == "save_results":
                    continue
                raise ValueError(f"Unknown stage '{stage}'")
            filtered.append(command_map[stage])
        return tuple(filtered)


__all__ = ["StageFactory"]
