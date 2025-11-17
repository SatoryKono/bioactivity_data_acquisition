"""Shim package that re-exports ``bioetl.application.runtime``."""

from __future__ import annotations

import importlib
import sys
from typing import Sequence

_TARGET = "bioetl.application.runtime"
_SUBMODULES: Sequence[str] = (
    "cli_base",
    "cli_errors",
    "cli_feedback",
    "cli_pipeline_runner",
    "errors",
    "lazy_loader",
    "load_meta_store",
)

_target_pkg = importlib.import_module(_TARGET)
__all__ = getattr(_target_pkg, "__all__", ())
globals().update({name: getattr(_target_pkg, name) for name in __all__})

for module_name in _SUBMODULES:
    sys.modules[f"{__name__}.{module_name}"] = importlib.import_module(
        f"{_TARGET}.{module_name}"
    )
