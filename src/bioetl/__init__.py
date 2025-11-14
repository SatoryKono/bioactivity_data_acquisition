"""Public interface for BioETL configuration and pipelines."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = ["PipelineConfig", "load_config"]


def __getattr__(name: str) -> Any:
    if name in __all__:
        config_module = import_module("bioetl.config")
        return getattr(config_module, name)
    msg = f"module 'bioetl' has no attribute '{name}'"
    raise AttributeError(msg)
