"""Совместимый namespace для CLI-инструментов `bioetl.cli.tools`."""

from __future__ import annotations

import sys
from importlib import import_module
from types import ModuleType
from typing import Final

from bioetl.cli.tool_specs import TOOL_COMMAND_SPECS

_TOOL_ALIAS_TO_MODULE: Final[dict[str, str]] = {
    **{spec.code: spec.implementation for spec in TOOL_COMMAND_SPECS},
    "qc_boundary": "bioetl.cli.tools.cli_qc_boundary",
}

__all__ = sorted(_TOOL_ALIAS_TO_MODULE.keys())


def _load_module(module_name: str) -> ModuleType:
    """Импортировать модуль CLI-инструмента с ленивой загрузкой."""

    return import_module(module_name)


for alias, module_name in _TOOL_ALIAS_TO_MODULE.items():
    module = _load_module(module_name)
    if hasattr(module, "cli_main") and not hasattr(module, "main"):
        module.main = module.cli_main  # type: ignore[attr-defined]
    globals()[alias] = module
    sys.modules[f"{__name__}.{alias}"] = module