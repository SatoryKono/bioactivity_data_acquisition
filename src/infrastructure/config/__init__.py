"""Configuration utilities for BioETL pipelines."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from infrastructure.runtime.lazy_loader import resolve_lazy_attr

__all__ = [
    "ActivitySourceConfig",
    "ActivitySourceParameters",
    "EnvironmentSettings",
    "AssaySourceConfig",
    "AssaySourceParameters",
    "apply_runtime_overrides",
    "build_env_override_mapping",
    "build_env_override_mapping",
    "DocumentSourceConfig",
    "DocumentSourceParameters",
    "PipelineConfig",
    "load_config",
    "load_environment_settings",
    "resolve_env_layers",
    "TargetSourceConfig",
    "TargetSourceParameters",
    "TestItemSourceConfig",
    "TestItemSourceParameters",
]

_LAZY_ATTRS = {
    "ActivitySourceConfig": "infrastructure.config.activity",
    "ActivitySourceParameters": "infrastructure.config.activity",
    "AssaySourceConfig": "infrastructure.config.assay",
    "AssaySourceParameters": "infrastructure.config.assay",
    "DocumentSourceConfig": "infrastructure.config.document",
    "DocumentSourceParameters": "infrastructure.config.document",
    "TargetSourceConfig": "infrastructure.config.target",
    "TargetSourceParameters": "infrastructure.config.target",
    "TestItemSourceConfig": "infrastructure.config.testitem",
    "TestItemSourceParameters": "infrastructure.config.testitem",
    "EnvironmentSettings": "infrastructure.config.environment",
    "apply_runtime_overrides": "infrastructure.config.environment",
    "build_env_override_mapping": "infrastructure.config.environment",
    "resolve_env_layers": "infrastructure.config.environment",
    "load_environment_settings": "infrastructure.config.environment",
    "PipelineConfig": "infrastructure.config.models.models",
    "load_config": "infrastructure.config.loader",
}


_CACHEABLE_EXPORTS = frozenset(_LAZY_ATTRS.keys())
_lazy_resolver = resolve_lazy_attr(
    globals(), _LAZY_ATTRS, cache=_CACHEABLE_EXPORTS
)


def __getattr__(name: str) -> Any:
    return _lazy_resolver(name)


def __dir__() -> list[str]:
    return sorted(set(__all__) | set(globals().keys()))


if TYPE_CHECKING:
    from .activity import ActivitySourceConfig, ActivitySourceParameters
    from .assay import AssaySourceConfig, AssaySourceParameters
    from .document import DocumentSourceConfig, DocumentSourceParameters
    from .environment import (
        EnvironmentSettings,
        apply_runtime_overrides,
        build_env_override_mapping,
        load_environment_settings,
        resolve_env_layers,
    )
    from .loader import load_config
    from .models.models import PipelineConfig
    from .target import TargetSourceConfig, TargetSourceParameters
    from .testitem import TestItemSourceConfig, TestItemSourceParameters
