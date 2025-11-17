"""Shim package that re-exports ``bioetl.domain.schema``."""

from __future__ import annotations

import importlib
import sys
from typing import Sequence

_TARGET = "bioetl.domain.schema"
_SUBMODULES: Sequence[str] = (
    "column_factory",
    "normalizers",
    "validation",
    "vocabulary_bindings",
)

_target_pkg = importlib.import_module(_TARGET)
__all__ = getattr(_target_pkg, "__all__", ())
globals().update({name: getattr(_target_pkg, name) for name in __all__})

for module_name in _SUBMODULES:
    sys.modules[f"{__name__}.{module_name}"] = importlib.import_module(
        f"{_TARGET}.{module_name}"
    )
