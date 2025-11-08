"""Typed configuration helpers for the ChEMBL document pipeline."""

from __future__ import annotations

from typing import ClassVar

from ..pipeline_source import (
    BaseSourceParameters,
    ChemblPipelineSourceConfig,
    SourceConfigDefaults,
)


class DocumentSourceParameters(BaseSourceParameters):
    """Параметры источника для document (используются общие поля)."""


class DocumentSourceConfig(ChemblPipelineSourceConfig[DocumentSourceParameters]):
    """Пайплайновая обёртка SourceConfig для document."""

    parameters_model: ClassVar[type[BaseSourceParameters]] = DocumentSourceParameters
    defaults: ClassVar[SourceConfigDefaults] = SourceConfigDefaults(page_size=25)


__all__ = ["DocumentSourceConfig", "DocumentSourceParameters"]
