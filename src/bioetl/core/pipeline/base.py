"""Compatibility exports for pipeline base classes."""

from __future__ import annotations

from .common import (
    PipelineBaseCommon as PipelineBase,
    SCHEMA_MIGRATION_REGISTRY,
)

__all__ = ["PipelineBase", "SCHEMA_MIGRATION_REGISTRY"]
