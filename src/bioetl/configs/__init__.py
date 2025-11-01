"""Configuration models and loader facade for packaged YAML profiles."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from bioetl.config.loader import deep_merge as _deep_merge
from bioetl.config.loader import load_config as _legacy_load_config
from bioetl.config.loader import parse_cli_overrides as _parse_cli_overrides
from bioetl.configs.models import (
    CacheConfig,
    DeterminismConfig,
    HttpConfig,
    PipelineConfig,
    RateLimitConfig,
    RetryConfig,
    Source,
)

__all__ = [
    "CacheConfig",
    "DeterminismConfig",
    "HttpConfig",
    "PipelineConfig",
    "RateLimitConfig",
    "RetryConfig",
    "Source",
    "load_pipeline_config",
    "parse_set_overrides",
]


def parse_set_overrides(overrides: Sequence[str]) -> dict[str, Any]:
    """Parse ``--set`` CLI overrides into a nested dictionary."""

    return _parse_cli_overrides(list(overrides))


def load_pipeline_config(
    config_path: str | Path,
    *,
    overrides: Mapping[str, Any] | None = None,
    set_overrides: Sequence[str] | None = None,
    env_prefix: str | Sequence[str] = ("BIOETL_",),
) -> PipelineConfig:
    """Load and validate a pipeline configuration.

    ``extends`` inheritance is resolved via the legacy loader, CLI overrides are
    applied via ``--set`` style arguments, and environment variables prefixed
    with ``BIOETL_`` (customisable via ``env_prefix``) provide the final layer of
    overrides. The resulting payload is re-validated against the strict models in
    :mod:`bioetl.configs.models` to guarantee ``ConfigDict(extra="forbid")``
    semantics for the exposed configuration surface.
    """

    config_path = Path(config_path)

    override_payload: dict[str, Any] = {}
    if overrides:
        override_payload = _deep_merge(override_payload, dict(overrides))

    if set_overrides:
        parsed = parse_set_overrides(set_overrides)
        override_payload = _deep_merge(override_payload, parsed)

    legacy = _legacy_load_config(
        config_path,
        overrides=override_payload,
        env_prefix=env_prefix,
    )

    payload = legacy.model_dump(mode="python", exclude_unset=False)
    config = PipelineConfig.model_validate(payload)
    config.attach_source_path(legacy.source_path)
    return config
