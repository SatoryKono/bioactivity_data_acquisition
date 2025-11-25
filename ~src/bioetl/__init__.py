"""Public interface for BioETL."""
from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "PipelineConfig",
    "load_config",
    "BaseApiClient",
    "IParser",
    "INormalizer",
    "UnifiedLogger",
    "PipelineBase",
    "RunResult",
]

_EXPORTS: dict[str, tuple[str, str]] = {
    "PipelineConfig": ("bioetl.config", "PipelineConfig"),
    "load_config": ("bioetl.config", "load_config"),
    "BaseApiClient": ("bioetl.base_classes", "BaseApiClient"),
    "IParser": ("bioetl.base_classes", "IParser"),
    "INormalizer": ("bioetl.base_classes", "INormalizer"),
    "UnifiedLogger": ("bioetl.core.logging", "UnifiedLogger"),
    "PipelineBase": ("bioetl.core.pipeline", "PipelineBase"),
    "RunResult": ("bioetl.core.pipeline", "RunResult"),
}


def __getattr__(name: str) -> Any:
    try:
        module_name, attr_name = _EXPORTS[name]
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise AttributeError(name) from exc

    module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
