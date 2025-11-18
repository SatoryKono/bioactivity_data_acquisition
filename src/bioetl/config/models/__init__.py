"""Deprecated namespace package for configuration models.

The legacy module :mod:`bioetl.config.models` used to re-export commonly
used dataclasses.  Modern code should import explicitly from
``bioetl.config.models.models`` (for configuration objects) or
``bioetl.config.models.policies`` (for policy helpers).  This shim only
exists to keep old imports working temporarily and will be removed once
all downstream projects are migrated.
"""

from __future__ import annotations

from importlib import import_module
from types import ModuleType
from typing import Any

import warnings

__all__: list[str] = []

_DEPRECATION_MESSAGE = (
    "Importing from 'bioetl.config.models' is deprecated. "
    "Import directly from 'bioetl.config.models.models' or "
    "'bioetl.config.models.policies' instead."
)

_FORWARD_MODULES: tuple[str, ...] = (
    "bioetl.config.models.models",
    "bioetl.config.models.policies",
)

_MODULE_CACHE: dict[str, ModuleType] = {}


def _load_module(module_name: str) -> ModuleType:
    module = _MODULE_CACHE.get(module_name)
    if module is None:
        module = import_module(module_name)
        _MODULE_CACHE[module_name] = module
    return module


def __getattr__(name: str) -> Any:
    warnings.warn(_DEPRECATION_MESSAGE, DeprecationWarning, stacklevel=2)
    for module_name in _FORWARD_MODULES:
        module = _load_module(module_name)
        if hasattr(module, name):
            return getattr(module, name)
    msg = f"module 'bioetl.config.models' has no attribute '{name}'"
    raise AttributeError(msg)
