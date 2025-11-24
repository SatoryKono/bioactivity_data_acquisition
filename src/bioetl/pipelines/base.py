from __future__ import annotations

"""Pipeline orchestration layer reused across core and pipelines packages."""

from bioetl.core.pipeline.factory import StageFactory
from bioetl.core.pipeline.orchestration import PipelineBaseCommon
from bioetl.core.pipeline.types import (
    PipelineExtractionMode,
    PipelineStageCommand,
    PipelineStagesProtocol,
    RunResult,
    StageContext,
    StageExecutionOptions,
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
