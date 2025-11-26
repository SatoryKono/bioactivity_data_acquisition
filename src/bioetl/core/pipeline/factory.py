"""StageFactory for building pipeline stage plans."""
from __future__ import annotations

from collections.abc import Sequence

from bioetl.core.pipeline.definition import PipelineDefinition
from bioetl.core.pipeline.types import Stage, StageContext, StageExecutionOptions


class StageFactory:
    """Factory that builds a sequence of :class:`Stage` objects from a definition."""

    def __init__(self, definition: PipelineDefinition) -> None:
        self.definition = definition

    def build(
        self,
        context: StageContext,
        options: StageExecutionOptions,
        stages: Sequence[str] | None = None,
    ) -> tuple[Stage, ...]:
        """Build a stage plan.

        Args:
            options: Shared execution options.
            stages: Explicit list of stages to include. If ``None`` the default
                pipeline plan is used.
        """

        self.definition.validate()
        stage_plan: tuple[Stage, ...] = self.definition.stages

        if stages is None:
            filtered_plan = stage_plan
        else:
            command_map = {command.name: command for command in stage_plan}
            filtered: list[Stage] = []
            for stage in stages:
                if stage not in command_map:
                    if options.dry_run and stage == "save_results":
                        continue
                    msg = f"Unknown stage '{stage}'"
                    raise ValueError(msg)
                filtered.append(command_map[stage])
            filtered_plan = tuple(filtered)

        if options.dry_run:
            filtered_plan = tuple(command for command in filtered_plan if command.name != "save_results")
            if getattr(context.pipeline, "validator", None) is None:
                filtered_plan = tuple(command for command in filtered_plan if command.name == "extract")

        return filtered_plan


__all__ = ["StageFactory"]
