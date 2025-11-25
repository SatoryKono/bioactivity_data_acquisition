"""Pipeline orchestration interfaces."""
from __future__ import annotations

from collections.abc import Callable

from bioetl.core.pipeline.orchestration import PipelineBaseCommon
from bioetl.core.pipeline.types import PipelineStagesProtocol

# Backwards-compatible alias used by CLI/tests
PipelineBase = PipelineBaseCommon
PipelineFactory = Callable[..., PipelineBase]

__all__ = ["PipelineBase", "PipelineStagesProtocol", "PipelineFactory"]
