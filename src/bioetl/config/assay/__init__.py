"""Typed configuration helpers for the ChEMBL assay pipeline."""

from __future__ import annotations

from typing import ClassVar

from ..pipeline_source import (
    BaseSourceParameters,
    ChemblPipelineSourceConfig,
    SourceConfigDefaults,
)


class AssaySourceParameters(BaseSourceParameters):
    """Параметры источника для assay (используются общие поля)."""


class AssaySourceConfig(ChemblPipelineSourceConfig[AssaySourceParameters]):
    """Пайплайновая обёртка SourceConfig для assay."""

    parameters_model: ClassVar[type[BaseSourceParameters]] = AssaySourceParameters
    defaults: ClassVar[SourceConfigDefaults] = SourceConfigDefaults(
        page_size=25,
        page_size_cap=25,
        max_url_length=2000,
        max_url_length_cap=2000,
        handshake_endpoint="/status",
        handshake_enabled=True,
    )


__all__ = ["AssaySourceConfig", "AssaySourceParameters"]
