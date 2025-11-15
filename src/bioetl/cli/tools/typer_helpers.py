"""Обратная совместимость для Typer-хелперов CLI."""

from __future__ import annotations

import importlib as _importlib

from bioetl.cli.cli_entrypoint import (
    TyperApp,
    create_app,
    create_simple_tool_app,
    get_typer,
    register_tool_app,
    run_app,
)
from bioetl.cli._typer_loader import _load_typer

importlib = _importlib

__all__ = [
    "TyperApp",
    "create_app",
    "create_simple_tool_app",
    "get_typer",
    "register_tool_app",
    "run_app",
    "importlib",
]

__all__.append("_load_typer")

