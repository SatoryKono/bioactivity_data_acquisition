from __future__ import annotations

from typing import TYPE_CHECKING

from bioetl.core.pipeline.stages import (
    _CleanupStageCommand,
    _ExtractStageCommand,
    _TransformStageCommand,
    _ValidateStageCommand,
    _WriteStageCommand,
)
from bioetl.core.pipeline.types import PipelineStageCommand

if TYPE_CHECKING:  # pragma: no cover - imported for typing only
    from bioetl.core.pipeline.orchestration import PipelineBaseCommon


class StageFactory:
    """Factory responsible for building the default stage plan."""

    def __init__(self, pipeline: PipelineBaseCommon) -> None:
        self.pipeline = pipeline

    def build(self) -> list[PipelineStageCommand]:
        return [
            _ExtractStageCommand(self.pipeline),
            _TransformStageCommand(self.pipeline),
            _ValidateStageCommand(self.pipeline),
            _WriteStageCommand(self.pipeline),
            _CleanupStageCommand(self.pipeline),
        ]


__all__ = ["StageFactory"]
