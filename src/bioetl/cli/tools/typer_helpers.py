"""Обратная совместимость для Typer-хелперов CLI."""

from __future__ import annotations

import importlib as _importlib
from typing import cast

from bioetl.cli.cli_entrypoint import (
    TyperApp,
    TyperModule,
    create_app,
    create_simple_tool_app,
    get_typer,
    register_tool_app,
    run_app,
)

importlib = _importlib

__all__ = [
    "TyperApp",
    "create_app",
    "create_simple_tool_app",
    "get_typer",
    "register_tool_app",
    "run_app",
    "_load_typer",
    "importlib",
]

_typer_module: TyperModule | None = None


def _load_typer() -> TyperModule:
    """Import ``typer`` for legacy helpers."""

    global _typer_module
    if _typer_module is not None:
        return _typer_module

    try:
        module = importlib.import_module("typer")
    except ModuleNotFoundError as exc:  # noqa: PERF203
        msg = "The `typer` dependency is unavailable. Install the `bioetl[cli]` extra."
        raise RuntimeError(msg) from exc

    _typer_module = cast(TyperModule, module)
    return _typer_module

