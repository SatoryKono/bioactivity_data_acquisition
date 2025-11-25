"""Lazy registry accessors to decouple CLI startup from heavy builders."""
from __future__ import annotations

from typing import Any, Callable, Dict, Iterable

from bioetl.cli.cli_registry import (
    COMMAND_REGISTRY,
    PIPELINE_REGISTRY,
    CommandConfig,
    PipelineCommandSpec,
)


def get_command_registry() -> Dict[str, Callable[[], Any]]:
    """Return a shallow copy of the command registry without executing builders."""
    return dict(COMMAND_REGISTRY)


def get_pipeline_specs() -> Iterable[PipelineCommandSpec]:
    """Expose pipeline specifications without triggering command factory execution."""
    return tuple(PIPELINE_REGISTRY)


def build_config_for(name: str) -> CommandConfig:
    """Build a command configuration by its registered name."""
    builder = COMMAND_REGISTRY.get(name)
    if builder is None:
        raise KeyError(name)
    return builder()
