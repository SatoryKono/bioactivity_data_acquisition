"""Deprecated shim; use :mod:`bioetl.core.config_contracts` instead."""

from __future__ import annotations

from .config_contracts import (
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
)

__all__ = [
    "PipelineCacheConfigProtocol",
    "PipelineCLIConfigProtocol",
    "PipelineConfigProtocol",
    "PipelineDeterminismConfigProtocol",
    "PipelineDeterminismEnvironmentProtocol",
    "PipelineDeterminismHashingProtocol",
    "PipelineDeterminismSortingProtocol",
    "PipelineMaterializationConfigProtocol",
    "PipelineMetadataProtocol",
    "PipelinePathsConfigProtocol",
    "PipelineTransformConfigProtocol",
    "PipelineValidationConfigProtocol",
    "SourceConfigProtocol",
]
