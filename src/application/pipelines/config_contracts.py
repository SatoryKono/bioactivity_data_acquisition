"""Typed contracts bridging configuration models и пайплайны.

Модуль выделен отдельно, чтобы `infrastructure.config` и `application.pipelines`
не импортировали друг друга напрямую. Протоколы описывают минимальный
набор атрибутов, ожидаемый `PipelineBase`.

>>> from infrastructure.config.core_contracts import PipelineConfigProtocol
>>> hasattr(PipelineConfigProtocol, "__module__")
True
"""

from __future__ import annotations

from infrastructure.config.core.contracts import (
    PipelineCacheConfigProtocol,
    PipelineCLIConfigProtocol,
    PipelineConfigProtocol,
    PipelineDeterminismConfigProtocol,
    PipelineDeterminismEnvironmentProtocol,
    PipelineDeterminismHashingProtocol,
    PipelineDeterminismSortingProtocol,
    PipelineMaterializationConfigProtocol,
    PipelineMetadataProtocol,
    PipelinePathsConfigProtocol,
    PipelineTransformConfigProtocol,
    PipelineValidationConfigProtocol,
    SourceConfigProtocol,
    SupportsModelCopy,
)

__all__ = [
    "PipelineCacheConfigProtocol",
    "PipelineCLIConfigProtocol",
    "PipelineDeterminismConfigProtocol",
    "PipelineDeterminismEnvironmentProtocol",
    "PipelineDeterminismHashingProtocol",
    "PipelineDeterminismSortingProtocol",
    "PipelineMaterializationConfigProtocol",
    "PipelineMetadataProtocol",
    "PipelinePathsConfigProtocol",
    "PipelineTransformConfigProtocol",
    "PipelineValidationConfigProtocol",
    "PipelineConfigProtocol",
    "SourceConfigProtocol",
    "SupportsModelCopy",
]
