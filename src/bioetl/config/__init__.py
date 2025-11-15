"""Configuration utilities for BioETL pipelines."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from bioetl.core.runtime.lazy_loader import resolve_lazy_attr

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
    "ActivitySourceConfig": "bioetl.config.activity",
    "ActivitySourceParameters": "bioetl.config.activity",
    "AssaySourceConfig": "bioetl.config.assay",
    "AssaySourceParameters": "bioetl.config.assay",
    "DocumentSourceConfig": "bioetl.config.document",
    "DocumentSourceParameters": "bioetl.config.document",
    "TargetSourceConfig": "bioetl.config.target",
    "TargetSourceParameters": "bioetl.config.target",
    "TestItemSourceConfig": "bioetl.config.testitem",
    "TestItemSourceParameters": "bioetl.config.testitem",
    "EnvironmentSettings": "bioetl.config.environment",
    "apply_runtime_overrides": "bioetl.config.environment",
    "build_env_override_mapping": "bioetl.config.environment",
    "resolve_env_layers": "bioetl.config.environment",
    "load_environment_settings": "bioetl.config.environment",
    "PipelineConfig": "bioetl.config.models",
    "load_config": "bioetl.config.loader",
}


_CACHEABLE_EXPORTS = frozenset(_LAZY_ATTRS.keys())
_lazy_resolver = resolve_lazy_attr(globals(), _LAZY_ATTRS, cache=_CACHEABLE_EXPORTS)


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
