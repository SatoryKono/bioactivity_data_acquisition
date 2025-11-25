from __future__ import annotations

from collections.abc import Callable

from bioetl.config import PipelineConfig

PipelineFactory = Callable[[PipelineConfig, str | None], object]

PIPELINE_REGISTRY: dict[str, PipelineFactory] = {}


def register_pipeline(name: str, factory: PipelineFactory) -> None:
    if not name:
        raise ValueError("pipeline name must be provided")
    PIPELINE_REGISTRY[name] = factory


__all__ = ["PIPELINE_REGISTRY", "register_pipeline", "PipelineFactory"]
