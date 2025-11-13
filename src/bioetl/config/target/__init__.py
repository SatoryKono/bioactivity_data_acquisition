"""Typed configuration helpers for the ChEMBL target pipeline."""

from __future__ import annotations

from typing import ClassVar

from ..pipeline_source import (
    BaseSourceParameters,
    ChemblPipelineSourceConfig,
    SourceConfigDefaults,
)


class TargetSourceParameters(BaseSourceParameters):
    """Параметры источника для target (используются общие поля)."""


class TargetSourceConfig(ChemblPipelineSourceConfig[TargetSourceParameters]):
    """Пайплайновая обёртка SourceConfig для target."""

    parameters_model: ClassVar[type[BaseSourceParameters]] = TargetSourceParameters
    defaults: ClassVar[SourceConfigDefaults] = SourceConfigDefaults(page_size=25)


__all__ = ["TargetSourceConfig", "TargetSourceParameters"]
