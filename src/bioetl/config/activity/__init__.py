"""Typed configuration helpers for the ChEMBL activity pipeline."""

from __future__ import annotations

from typing import ClassVar

from ..pipeline_source import (
    BaseSourceParameters,
    ChemblPipelineSourceConfig,
    SourceConfigDefaults,
)


class ActivitySourceParameters(BaseSourceParameters):
    """Параметры источника для activity (используются общие поля)."""


class ActivitySourceConfig(ChemblPipelineSourceConfig[ActivitySourceParameters]):
    """Пайплайновая обёртка SourceConfig для activity."""

    parameters_model: ClassVar[type[BaseSourceParameters]] = ActivitySourceParameters
    defaults: ClassVar[SourceConfigDefaults] = SourceConfigDefaults(page_size=25)


__all__ = ["ActivitySourceConfig", "ActivitySourceParameters"]
