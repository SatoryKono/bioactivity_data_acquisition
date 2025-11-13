"""Compatibility layer for legacy base pipeline imports.

The module is kept to support code that still imports classes from
``bioetl.core.base_pipeline``. All functionality was moved to
``bioetl.pipelines.base``, therefore this module only performs re-exports.
"""

from __future__ import annotations

from bioetl.core.io import RunArtifacts, WriteArtifacts, WriteResult
from bioetl.pipelines.base import PipelineBase, RunResult

__all__ = [
    "PipelineBase",
    "RunArtifacts",
    "RunResult",
    "WriteArtifacts",
    "WriteResult",
]

