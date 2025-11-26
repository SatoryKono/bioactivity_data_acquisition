"""Pipeline orchestration interfaces."""
from __future__ import annotations

import warnings
from collections.abc import Callable

from bioetl.core.pipeline.unified import UnifiedPipelineBase
from bioetl.core.pipeline.types import PipelineStagesProtocol

warnings.warn(
    "The 'bioetl.pipelines.base' module is deprecated and will be removed. "
    "Use 'bioetl.core.pipeline.unified' instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Backwards-compatible alias used by CLI/tests
PipelineBase = UnifiedPipelineBase
PipelineFactory = Callable[..., PipelineBase]

__all__ = ["PipelineBase", "PipelineStagesProtocol", "PipelineFactory"]
