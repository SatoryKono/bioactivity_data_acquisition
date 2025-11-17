"""Shim package that re-exports ``bioetl.infrastructure.http``."""

from __future__ import annotations

import importlib
import sys
from typing import Sequence

_TARGET = "bioetl.infrastructure.http"
_SUBMODULES: Sequence[str] = (
    "api_client",
    "client_factory",
)

_target_pkg = importlib.import_module(_TARGET)
__all__ = getattr(_target_pkg, "__all__", ())
globals().update({name: getattr(_target_pkg, name) for name in __all__})

for module_name in _SUBMODULES:
    sys.modules[f"{__name__}.{module_name}"] = importlib.import_module(
        f"{_TARGET}.{module_name}"
    )
