"""Shared helper utilities for BioETL CLI tools."""

from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Protocol

import typer

__all__ = [
    "get_project_root",
    "ToolRunnable",
    "load_typer_app",
]


def get_project_root() -> Path:
    """Return the absolute repository root."""

    return Path(__file__).resolve().parents[3]


class ToolRunnable(Protocol):
    """Protocol for callables that implement CLI tool business logic."""

    def __call__(self, *args: object, **kwargs: object) -> object:
        ...


def load_typer_app(module_path: str, app_name: str = "app") -> typer.Typer:
    """Load a Typer application object by module path."""

    module = import_module(module_path)
    app = getattr(module, app_name, None)
    if app is None:
        raise RuntimeError(f"Module {module_path} does not define Typer app '{app_name}'")
    if not isinstance(app, typer.Typer):
        raise TypeError(
            f"Attribute '{app_name}' of module {module_path} is not a Typer application; "
            f"got {type(app)!r}"
        )
    return app
