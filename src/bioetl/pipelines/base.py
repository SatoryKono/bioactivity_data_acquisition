"""Pipeline orchestration interfaces."""
from __future__ import annotations

from collections.abc import Callable

from bioetl.core.pipeline.unified import UnifiedPipelineBase
from bioetl.core.pipeline.types import PipelineStagesProtocol

# Backwards-compatible alias used by CLI/tests
PipelineBase = UnifiedPipelineBase
PipelineFactory = Callable[..., PipelineBase]

__all__ = ["PipelineBase", "PipelineStagesProtocol", "PipelineFactory"]
