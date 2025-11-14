"""Configuration utilities for BioETL pipelines."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

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


def __getattr__(name: str) -> Any:
    module_name = _LAZY_ATTRS.get(name)
    if module_name is None:
        raise AttributeError(name)
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value


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
