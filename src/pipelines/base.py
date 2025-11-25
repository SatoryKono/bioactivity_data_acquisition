from __future__ import annotations

from bioetl.pipelines.base import FileBasedPipeline, PipelineSpec, load_pipeline_config

PipelineFactory = tuple[type[FileBasedPipeline], PipelineSpec]

__all__ = [
    "FileBasedPipeline",
    "PipelineSpec",
    "PipelineFactory",
    "load_pipeline_config",
]
