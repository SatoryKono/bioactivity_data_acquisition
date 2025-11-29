"""Registry for pipeline factories used by the CLI."""
from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from bioetl.core.config import PipelineConfig
from bioetl.core.pipeline.types import PipelineBaseProtocol

PipelineFactory = Callable[[PipelineConfig, str | None], PipelineBaseProtocol]

PIPELINE_REGISTRY: dict[str, PipelineFactory] = {}


def register_pipeline(name: str, factory: PipelineFactory) -> None:
    """Register a pipeline factory with the global registry."""
    if not name:
        raise ValueError("pipeline name must be provided")
    PIPELINE_REGISTRY[name] = factory


def _wrap_pipeline_factory(pipeline_cls: type[Any]) -> PipelineFactory:
    """Create a factory function that instantiates a pipeline class."""

    def factory(
        config: PipelineConfig, run_id: str | None = None
    ) -> PipelineBaseProtocol:
        payload = (
            config.model_dump()
            if hasattr(config, "model_dump")
            else dict(config)
        )
        return cast(
            PipelineBaseProtocol, pipeline_cls(payload, run_id=run_id)
        )

    return factory


def _register_default_pipelines() -> None:
    # Регистрация по умолчанию отключена в новой архитектуре, чтобы
    # избегать автоподключения устаревших пайплайнов.
    return


_register_default_pipelines()


__all__ = ["PIPELINE_REGISTRY", "register_pipeline", "PipelineFactory"]
