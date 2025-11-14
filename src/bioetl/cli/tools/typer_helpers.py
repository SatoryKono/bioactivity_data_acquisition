"""Обратная совместимость для Typer-хелперов CLI."""

from __future__ import annotations

import importlib as _importlib

from typing import Any, Callable

from bioetl.cli.cli_entrypoint import (
    TyperApp,
    TyperModule,
    create_app,
    create_simple_tool_app,
    get_typer,
    register_tool_app,
    run_app,
    _load_typer as _entrypoint_load_typer,
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

_load_typer: Callable[[], TyperModule] = _entrypoint_load_typer

