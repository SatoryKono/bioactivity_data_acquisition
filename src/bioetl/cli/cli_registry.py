"""Static registry of pipeline factories for the CLI layer.

The registry keeps orchestration concerns decoupled from pipeline
implementations and allows dynamic command generation.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Dict

from bioetl.pipelines.base import PipelineFactory

PIPELINE_REGISTRY: Dict[str, PipelineFactory] = {}
"""Mapping of pipeline command names to pipeline factories."""


def register_pipeline(name: str, factory: PipelineFactory) -> None:
    """Register a pipeline factory for CLI discovery.

    Parameters
    ----------
    name:
        Public command name that will be exposed via the CLI.
    factory:
        Callable that returns an initialized :class:`~bioetl.pipelines.base.PipelineBase`.
    """

    PIPELINE_REGISTRY[name] = factory
