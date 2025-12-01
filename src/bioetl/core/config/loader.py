from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from .config_resolver import FileConfigResolver
from .models import PipelineConfig


def load_config(
    config_path: str | Path,
    *,
    profiles: Sequence[str | Path] | None = None,
    cli_overrides: Mapping[str, Any] | None = None,
    env: Mapping[str, str] | None = None,
    env_prefixes: Sequence[str] = ("BIOETL__",),
    include_default_profiles: bool = False,
) -> PipelineConfig:
    """Load, merge and validate the pipeline configuration."""

    resolver = FileConfigResolver(
        config_path,
        profiles=profiles,
        env=env,
        env_prefixes=env_prefixes,
        include_default_profiles=include_default_profiles,
    )
    return resolver.resolve(overrides=cli_overrides)


__all__ = ["load_config", "PipelineConfig"]
