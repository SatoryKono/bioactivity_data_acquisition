"""Typed configuration helpers for the ChEMBL testitem pipeline."""

from __future__ import annotations

from typing import ClassVar

from ..pipeline_source import (
    BaseSourceParameters,
    ChemblPipelineSourceConfig,
    SourceConfigDefaults,
)


class TestItemSourceParameters(BaseSourceParameters):
    """Параметры источника для testitem (используются общие поля)."""


class TestItemSourceConfig(ChemblPipelineSourceConfig[TestItemSourceParameters]):
    """Пайплайновая обёртка SourceConfig для testitem."""

    parameters_model: ClassVar[type[BaseSourceParameters]] = TestItemSourceParameters
    defaults: ClassVar[SourceConfigDefaults] = SourceConfigDefaults(
        page_size=200,
        page_size_cap=25,
        max_url_length=2000,
        max_url_length_cap=2000,
        handshake_endpoint="/status",
        handshake_enabled=True,
    )


__all__ = ["TestItemSourceConfig", "TestItemSourceParameters"]
