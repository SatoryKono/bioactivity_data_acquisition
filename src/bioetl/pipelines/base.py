from __future__ import annotations

"""Pipeline orchestration layer reused across core and pipelines packages."""

from bioetl.core.pipeline.orchestration import (
    PipelineBaseCommon,
    PipelineExtractionMode,
    PipelineStageCommand,
    PipelineStagesProtocol,
    RunResult,
    StageContext,
    StageExecutionOptions,
    StageFactory,
)


class PipelineBase(PipelineBaseCommon):
    """Pipeline base used by pipelines implementations."""

    # Override points can be added here if pipeline-specific behaviour diverges
    pass


__all__ = [
    "PipelineBase",
    "PipelineBaseCommon",
    "PipelineExtractionMode",
    "PipelineStageCommand",
    "PipelineStagesProtocol",
    "RunResult",
    "StageContext",
    "StageExecutionOptions",
    "StageFactory",
]
